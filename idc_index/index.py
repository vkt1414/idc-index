from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
import tempfile
from importlib.metadata import distribution

import time
from pathlib import Path

import duckdb
import idc_index_data
import pandas as pd
import psutil
from packaging.version import Version
from tqdm import tqdm

logger = logging.getLogger(__name__)

aws_endpoint_url = "https://s3.amazonaws.com"
gcp_endpoint_url = "https://storage.googleapis.com"


class IDCClient:
    def __init__(self):
        file_path = idc_index_data.IDC_INDEX_PARQUET_FILEPATH

        # Read index file
        logger.debug(f"Reading index file v{idc_index_data.__version__}")
        self.index = pd.read_parquet(file_path)
        self.index = self.index.astype(str).replace("nan", "")
        self.index["series_size_MB"] = self.index["series_size_MB"].astype(float)
        self.collection_summary = self.index.groupby("collection_id").agg(
            {"Modality": pd.Series.unique, "series_size_MB": "sum"}
        )

        # Lookup s5cmd
        self.s5cmdPath = shutil.which("s5cmd")

        if self.s5cmdPath is None:
            # Workaround to support environment without a properly setup PATH
            # See https://github.com/Slicer/Slicer/pull/7587
            logger.debug("Falling back to looking up s5cmd along side the package")
            for script in distribution("s5cmd").files:
                if str(script).startswith("s5cmd/bin/s5cmd"):
                    self.s5cmdPath = script.locate().resolve(strict=True)
                    break

        if self.s5cmdPath is None:
            raise FileNotFoundError(
                "s5cmd executable not found. Please install s5cmd from https://github.com/peak/s5cmd#installation"
            )

        self.s5cmdPath = str(self.s5cmdPath)

        logger.debug(f"Found s5cmd executable: {self.s5cmdPath}")

        # ... and check it can be executed
        subprocess.check_call([self.s5cmdPath, "--help"], stdout=subprocess.DEVNULL)

    @staticmethod
    def _filter_dataframe_by_id(key, dataframe, _id):
        values = _id
        if isinstance(_id, str):
            values = [_id]
        filtered_df = dataframe[dataframe[key].isin(values)].copy()
        if filtered_df.empty:
            error_message = f"No data found for the {key} with the values {values}."
            raise ValueError(error_message)
        return filtered_df

    @staticmethod
    def _filter_by_collection_id(df_index, collection_id):
        return IDCClient._filter_dataframe_by_id(
            "collection_id", df_index, collection_id
        )

    @staticmethod
    def _filter_by_patient_id(df_index, patient_id):
        return IDCClient._filter_dataframe_by_id("PatientID", df_index, patient_id)

    @staticmethod
    def _filter_by_dicom_study_uid(df_index, dicom_study_uid):
        return IDCClient._filter_dataframe_by_id(
            "StudyInstanceUID", df_index, dicom_study_uid
        )

    @staticmethod
    def _filter_by_dicom_series_uid(df_index, dicom_series_uid):
        return IDCClient._filter_dataframe_by_id(
            "SeriesInstanceUID", df_index, dicom_series_uid
        )

    @staticmethod
    def get_idc_version():
        """
        Returns the version of IDC data used in idc-index
        """
        idc_version = Version(idc_index_data.__version__).major
        return f"v{idc_version}"

    def get_collections(self):
        """
        Returns the collections present in IDC
        """
        unique_collections = self.index["collection_id"].unique()
        return unique_collections.tolist()

    def get_series_size(self, seriesInstanceUID):
        """
        Gets cumulative size (MB) of the DICOM instances in a given SeriesInstanceUID.
        Args:
            seriesInstanceUID (str): The DICOM SeriesInstanceUID.
        Returns:
            float: The cumulative size of the DICOM instances in the given SeriesInstanceUID rounded to two digits, in MB.
        Raises:
            ValueError: If the `seriesInstanceUID` does not exist.
        """

        resp = self.index[["SeriesInstanceUID"] == seriesInstanceUID][
            "series_size_MB"
        ].iloc[0]
        return resp

    def get_patients(self, collection_id, outputFormat="dict"):
        """
        Gets the patients in a collection.
        Args:
            collection_id (str or a list of str): The collection id or list of collection ids. This should be in lower case separated by underscores.
                                For example, 'pdmr_texture_analysis'. or ['pdmr_texture_analysis','nlst']
            outputFormat (str, optional): The format in which to return the patient IDs. Available options are 'dict',
                                        'df', and 'list'. Default is 'dict'.
        Returns:
            dict or pandas.DataFrame or list: Patient IDs in the requested output format. By default, it returns a dictionary.
        Raises:
            ValueError: If `outputFormat` is not one of 'dict', 'df', 'list'.
        """

        if not isinstance(collection_id, str) and not isinstance(collection_id, list):
            raise TypeError("collection_id must be a string or list of strings")

        if outputFormat not in ["dict", "df", "list"]:
            raise ValueError("outputFormat must be either 'dict', 'df', or 'list")

        patient_df = self._filter_by_collection_id(self.index, collection_id)

        if outputFormat == "list":
            response = patient_df["PatientID"].unique().tolist()
        else:
            patient_df = patient_df.rename(columns={"collection_id": "Collection"})
            patient_df = patient_df[["PatientID", "PatientSex", "PatientAge"]]
            patient_df = (
                patient_df.groupby("PatientID")
                .agg(
                    {
                        "PatientSex": lambda x: ",".join(x[x != ""].unique()),
                        "PatientAge": lambda x: ",".join(x[x != ""].unique()),
                    }
                )
                .reset_index()
            )

            patient_df = patient_df.drop_duplicates().sort_values(by="PatientID")
            # Convert DataFrame to a list of dictionaries for the API-like response
            if outputFormat == "dict":
                response = patient_df.to_dict(orient="records")
            else:
                response = patient_df

        logger.debug("Get patient response: %s", str(response))

        return response

    def get_dicom_studies(self, patientId, outputFormat="dict"):
        """
        Returns Studies for a given patient or list of patients.
        Args:
            patientId (str or list of str): The patient Id or a list of patient Ids.
            outputFormat (str, optional): The format in which to return the studies. Available options are 'dict',
                                        'df', and 'list'. Default is 'dict'.
        Returns:
            dict or pandas.DataFrame or list: Studies in the requested output format. By default, it returns a dictionary.
        Raises:
            ValueError: If `outputFormat` is not one of 'dict', 'df', 'list'.
            ValueError: If any of the `patientId` does not exist.
        """

        if not isinstance(patientId, str) and not isinstance(patientId, list):
            raise TypeError("patientId must be a string or list of strings")

        if outputFormat not in ["dict", "df", "list"]:
            raise ValueError("outputFormat must be either 'dict' or 'df' or 'list'")

        studies_df = self._filter_by_patient_id(self.index, patientId)

        if outputFormat == "list":
            response = studies_df["StudyInstanceUID"].unique().tolist()
        else:
            studies_df["patient_study_size_MB"] = studies_df.groupby(
                ["PatientID", "StudyInstanceUID"]
            )["series_size_MB"].transform("sum")
            studies_df["patient_study_series_count"] = studies_df.groupby(
                ["PatientID", "StudyInstanceUID"]
            )["SeriesInstanceUID"].transform("count")
            studies_df["patient_study_instance_count"] = studies_df.groupby(
                ["PatientID", "StudyInstanceUID"]
            )["instanceCount"].transform("count")

            studies_df = studies_df.rename(
                columns={
                    "collection_id": "Collection",
                    "patient_study_series_count": "SeriesCount",
                }
            )

            # patient_study_df = patient_study_df[['PatientID', 'PatientSex', 'Collection', 'PatientAge', 'StudyInstanceUID', 'StudyDate', 'StudyDescription', 'patient_study_size_MB', 'SeriesCount', 'patient_study_instance_count']]
            studies_df = studies_df[
                ["StudyInstanceUID", "StudyDate", "StudyDescription", "SeriesCount"]
            ]
            # Group by 'StudyInstanceUID'
            studies_df = (
                studies_df.groupby("StudyInstanceUID")
                .agg(
                    {
                        "StudyDate": lambda x: ",".join(x[x != ""].unique()),
                        "StudyDescription": lambda x: ",".join(x[x != ""].unique()),
                        "SeriesCount": lambda x: int(x[x != ""].iloc[0])
                        if len(x[x != ""]) > 0
                        else 0,
                    }
                )
                .reset_index()
            )

            studies_df = studies_df.drop_duplicates().sort_values(
                by=["StudyDate", "StudyDescription", "SeriesCount"]
            )

            if outputFormat == "dict":
                response = studies_df.to_dict(orient="records")
            else:
                response = studies_df

        logger.debug("Get patient study response: %s", str(response))

        return response

    def get_dicom_series(self, studyInstanceUID, outputFormat="dict"):
        """
        Returns Series for a given study or list of studies.
        Args:
            studyInstanceUID (str or list of str): The DICOM StudyInstanceUID or a list of StudyInstanceUIDs.
            outputFormat (str, optional): The format in which to return the series. Available options are 'dict',
                                        'df', and 'list'. Default is 'dict'.
        Returns:
            dict or pandas.DataFrame or list: Series in the requested output format. By default, it returns a dictionary.
        Raises:
            ValueError: If `outputFormat` is not one of 'dict', 'df', 'list'.
            ValueError: If any of the `studyInstanceUID` does not exist.
        """

        if not isinstance(studyInstanceUID, str) and not isinstance(
            studyInstanceUID, list
        ):
            raise TypeError("studyInstanceUID must be a string or list of strings")

        if outputFormat not in ["dict", "df", "list"]:
            raise ValueError("outputFormat must be either 'dict' or 'df' or 'list'")

        series_df = self._filter_by_dicom_study_uid(self.index, studyInstanceUID)

        if outputFormat == "list":
            response = series_df["SeriesInstanceUID"].unique().tolist()
        else:
            series_df = series_df.rename(
                columns={
                    "collection_id": "Collection",
                    "instanceCount": "instance_count",
                }
            )
            series_df["ImageCount"] = 1
            series_df = series_df[
                [
                    "StudyInstanceUID",
                    "SeriesInstanceUID",
                    "Modality",
                    "SeriesDate",
                    "Collection",
                    "BodyPartExamined",
                    "SeriesDescription",
                    "Manufacturer",
                    "ManufacturerModelName",
                    "series_size_MB",
                    "SeriesNumber",
                    "instance_count",
                    "ImageCount",
                ]
            ]

            series_df = series_df.drop_duplicates().sort_values(
                by=[
                    "Modality",
                    "SeriesDate",
                    "SeriesDescription",
                    "BodyPartExamined",
                    "SeriesNumber",
                ]
            )
            # Convert DataFrame to a list of dictionaries for the API-like response
            if outputFormat == "dict":
                response = series_df.to_dict(orient="records")
            else:
                response = series_df
        logger.debug("Get series response: %s", str(response))

        return response

    def _track_download_progress(
        self, size_MB: int, downloadDir: str, process: subprocess.Popen
    ):
        """
        Track progress by continuously checking the downloaded file size and updating the progress bar.
        """
        total_size_bytes = size_MB * 10**6  # Convert MB to bytes

        # Calculate the initial size of the directory
        initial_size_bytes = sum(
            f.stat().st_size for f in Path(downloadDir).iterdir() if f.is_file()
        )

        pbar = tqdm(
            total=total_size_bytes,
            unit="B",
            unit_scale=True,
            desc="Downloading data",
        )

        while True:
            downloaded_bytes = (
                sum(
                    f.stat().st_size for f in Path(downloadDir).iterdir() if f.is_file()
                )
                - initial_size_bytes
            )
            pbar.n = min(
                downloaded_bytes, total_size_bytes
            )  # Prevent the progress bar from exceeding 100%
            pbar.refresh()

            if process.poll() is not None:
                break

            time.sleep(0.5)

        # Wait for the process to finish
        stdout, stderr = process.communicate()
        pbar.close()

        # Check if download process completed successfully
        if process.returncode != 0:
            error_message = f"Download process failed: {stderr!s}"
            raise RuntimeError(error_message)

        logger.debug("Successfully downloaded files to %s", str(downloadDir))

    def download_dicom_series(self, seriesInstanceUID: str, downloadDir: str) -> None:
        """
        Download the files corresponding to the seriesInstanceUID to the specified directory.

        Returns: None

        """
        series_df = self.index[self.index["SeriesInstanceUID"] == seriesInstanceUID]
        if series_df.empty:
            error_message = (
                f"No series found with the SeriesInstanceUID '{seriesInstanceUID}'."
            )
            raise ValueError(error_message)

        # Start the download process
        series_url = self.index[self.index["SeriesInstanceUID"] == seriesInstanceUID][
            "series_aws_url"
        ].iloc[0]
        series_size_MB = self.index[
            self.index["SeriesInstanceUID"] == seriesInstanceUID
        ]["series_size_MB"].iloc[0]
        cmd = [
            self.s5cmdPath,
            "--no-sign-request",
            "--endpoint-url",
            aws_endpoint_url,
            "sync",
            series_url,
            downloadDir,
        ]
        with subprocess.Popen(
            cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True
        ) as process:
            # Track progress using tqdm
            self._track_download_progress(series_size_MB, downloadDir, process)

    def get_series_file_URLs(self, seriesInstanceUID):
        """
        Get the URLs of the files corresponding to the DICOM instances in a given SeriesInstanceUID.

        Args:
            SeriesInstanceUID: string containing the value of DICOM SeriesInstanceUID to filter by

        Returns:
            list of strings containing the AWS S3 URLs of the files corresponding to the SeriesInstanceUID
        """
        # Query to get the S3 URL
        s3url_query = f"""
        SELECT
          series_aws_url
        FROM
          index
        WHERE
          SeriesInstanceUID='{seriesInstanceUID}'
        """
        s3url_query_df = self.sql_query(s3url_query)
        s3_url = s3url_query_df.series_aws_url[0]

        # Remove the last character from the S3 URL
        s3_url = s3_url[:-1]

        # Run the s5cmd ls command and capture its output
        result = subprocess.run(
            [self.s5cmdPath, "--no-sign-request", "ls", s3_url],
            stdout=subprocess.PIPE,
            check=False,
        )
        output = result.stdout.decode("utf-8")

        # Parse the output to get the file names
        lines = output.split("\n")
        file_names = [s3_url + line.split()[-1] for line in lines if line]

        return file_names

    def get_viewer_URL(
        self, seriesInstanceUID=None, studyInstanceUID=None, viewer_selector=None
    ):
        """
        Get the URL of the IDC viewer for the given series or study in IDC based on
        the provided SeriesInstanceUID or StudyInstanceUID. If StudyInstanceUID is not provided,
        it will be automatically deduced. If viewer_selector is not provided, default viewers
        will be used (OHIF v2 or v3 for radiology modalities, and Slim for SM).

        This function will validate the provided SeriesInstanceUID or StudyInstanceUID against IDC
        index to ensure that the series or study is available in IDC.

        Args:
            SeriesInstanceUID: string containing the value of DICOM SeriesInstanceUID for a series
            available in IDC

            StudyInstanceUID: string containing the value of DICOM SeriesInstanceUID for a series
            available in IDC

            viewer_selector: string containing the name of the viewer to use. Must be one of the following:
            ohif_v2, ohif_v3 or slim. If not provided, default viewers will be used.

        Returns:
            string containing the IDC viewer URL for the given SeriesInstanceUID
        """

        if seriesInstanceUID is None and studyInstanceUID is None:
            raise ValueError(
                "Either SeriesInstanceUID or StudyInstanceUID, or both, must be provided."
            )

        if (
            seriesInstanceUID is not None
            and seriesInstanceUID not in self.index["SeriesInstanceUID"].values
        ):
            raise ValueError("SeriesInstanceUID not found in IDC index.")

        if (
            studyInstanceUID is not None
            and studyInstanceUID not in self.index["StudyInstanceUID"].values
        ):
            raise ValueError("StudyInstanceUID not found in IDC index.")

        if viewer_selector is not None and viewer_selector not in [
            "ohif_v2",
            "ohif_v3",
            "slim",
        ]:
            raise ValueError(
                "viewer_selector must be one of 'ohif_v2', 'ohif_v3',  or 'slim'."
            )

        modality = None

        if studyInstanceUID is None:
            query = f"""
            SELECT
                DISTINCT(StudyInstanceUID),
                Modality
            FROM
                index
            WHERE
                SeriesInstanceUID='{seriesInstanceUID}'
            """
            query_result = self.sql_query(query)
            studyInstanceUID = query_result.StudyInstanceUID[0]
            modality = query_result.Modality[0]

        else:
            query = f"""
            SELECT
                DISTINCT(Modality)
            FROM
                index
            WHERE
                StudyInstanceUID='{studyInstanceUID}'
            """
            query_result = self.sql_query(query)
            modality = query_result.Modality[0]

        if viewer_selector is None:
            if "SM" in modality:
                viewer_selector = "slim"
            else:
                viewer_selector = "ohif_v2"

        if viewer_selector == "ohif_v2":
            if seriesInstanceUID is None:
                viewer_url = f"https://viewer.imaging.datacommons.cancer.gov/viewer/{studyInstanceUID}"
            else:
                viewer_url = f"https://viewer.imaging.datacommons.cancer.gov/viewer/{studyInstanceUID}?SeriesInstanceUID={seriesInstanceUID}"
        elif viewer_selector == "ohif_v3":
            if seriesInstanceUID is None:
                viewer_url = f"https://viewer.imaging.datacommons.cancer.gov/v3/viewer/?StudyInstanceUIDs={studyInstanceUID}"
            else:
                viewer_url = f"https://viewer.imaging.datacommons.cancer.gov/v3/viewer/?StudyInstanceUIDs={studyInstanceUID}&SeriesInstanceUID={seriesInstanceUID}"
        elif viewer_selector == "volview":
            # TODO! Not implemented yet
            pass
        elif viewer_selector == "slim":
            if seriesInstanceUID is None:
                viewer_url = f"https://viewer.imaging.datacommons.cancer.gov/slim/studies/{studyInstanceUID}"
            else:
                viewer_url = f"https://viewer.imaging.datacommons.cancer.gov/slim/studies/{studyInstanceUID}/series/{seriesInstanceUID}"

        return viewer_url

    def _get_series_size_from_crdc_series_uuid(
        self, crdc_series_instance_uuid: str
    ) -> float:
        """
        Retrieves the size of a series from the index based on the given CRDC series instance UUID.
        As the index does only contains aws series urls, there is no direct way to
        get series size from a gcs url. However this function levarages the
        fact that both gcs and aws urls share the same folder name which is
        crdc series instance uuid.

        Args:
            crdc_series_instance_uuid (str): The UUID of the CRDC series instance.

        Returns:
            float: The size of the series in MB.
        """
        index = self.index
        series_size_sql = f"""
            SELECT
                series_size_MB
            FROM
                index
            WHERE
                series_aws_url LIKE '%{crdc_series_instance_uuid}%'
        """
        return duckdb.query(series_size_sql).to_df().series_size_MB.iloc[0]

    def _validate_manifest_and_get_download_size(
        self, manifestFile: str
    ) -> tuple[float, str]:
        """
        Validates the manifest file by checking the URLs and their availability.
        The function reads the manifest file line by line. For each line, it checks if
        the URL is valid and accessible.
        Uses the s5cmd to check the availability of the URLs in both AWS and GCP.
        If the URL is not accessible in either AWS or GCP, it raises a ValueError.
        In addition it also calculates the total size of all series in the manifest file.
        Args:
            manifestFile (str): The path to the manifest file.
        Returns:
            total_size (float): The total size of all series in the manifest file.
            endpoint_to_use (str): The endpoint URL to use (either AWS or GCP).
        Raises:
            ValueError: If the manifest file does not exist, if any URL in the manifest file is invalid, or if any URL is inaccessible in both AWS and GCP.
            Exception: If the manifest contains URLs from both AWS and GCP.
        """
        if not os.path.exists(manifestFile):
            raise ValueError("Manifest does not exist.")

        endpoint_to_use = None
        aws_found = False
        gcp_found = False
        total_size = 0

        with open(manifestFile) as f:
            for line in f:
                if not line.startswith("#"):
                    series_folder_pattern = r"(s3:\/\/.*)\/\*"
                    match = re.search(series_folder_pattern, line)
                    if match is None:
                        raise ValueError("Invalid URL format in manifest file.")
                    folder_url = match.group(1)

                    # Extract CRDC UUID from the line
                    crdc_series_uuid_pattern = r"(?:.*?\/){3}([^\/?#]+)"
                    match_uuid = re.search(crdc_series_uuid_pattern, line)
                    if match_uuid is None:
                        raise ValueError("Invalid URL format in manifest file.")
                    crdc_series_uuid = match_uuid.group(1)

                    # Check AWS endpoint
                    cmd = [
                        "s5cmd",
                        "--no-sign-request",
                        "--endpoint-url",
                        aws_endpoint_url,
                        "ls",
                        folder_url,
                    ]
                    process = subprocess.run(
                        cmd, capture_output=True, text=True, check=False
                    )
                    if process.stderr and process.stderr.startswith("ERROR"):
                        # Check GCP endpoint
                        cmd = [
                            "s5cmd",
                            "--no-sign-request",
                            "--endpoint-url",
                            gcp_endpoint_url,
                            "ls",
                            folder_url,
                        ]
                        process = subprocess.run(
                            cmd, capture_output=True, text=True, check=False
                        )
                        if process.stderr and process.stderr.startswith("ERROR"):
                            error_message = f"Manifest contains invalid or inaccessible URLs. Please check line '{line}'"
                            raise ValueError(error_message)
                        else:
                            if aws_found:
                                raise RuntimeError(
                                    "The manifest contains URLs from both AWS and GCP. Please use only one provider."
                                )
                            endpoint_to_use = gcp_endpoint_url
                            gcp_found = True
                    else:
                        if gcp_found:
                            raise RuntimeError(
                                "The manifest contains URLs from both AWS and GCP. Please use only one provider."
                            )
                        endpoint_to_use = aws_endpoint_url
                        aws_found = True

                    # Get the size of the series
                    series_size = self._get_series_size_from_crdc_series_uuid(
                        crdc_series_uuid
                    )
                    total_size += series_size
        if not endpoint_to_use:
            raise ValueError("No valid URLs found in the manifest.")

        return total_size, endpoint_to_use

    def download_from_manifest(
        self, manifestFile: str, downloadDir: str, quiet: bool = True
    ) -> None:
        """
        Download the manifest file. In a series of steps, the manifest file
        is first validated to ensure every line contains a valid urls. It then
        gets the total size to be downloaded and runs download process on one
        process and download progress on another process.

        Args:
            manifestFile (str): The path to the manifest file.
            downloadDir (str): The directory to download the files to.
            quiet (bool, optional): If True, suppresses the output of the subprocess. Defaults to True.

        Raises:
            ValueError: If the download directory does not exist.
        """
        total_size, endpoint_to_use = self._validate_manifest_and_get_download_size(
            manifestFile
        )
        print("Total size:" + str(total_size))
        downloadDir = os.path.abspath(downloadDir).replace("\\", "/")
        if not os.path.exists(downloadDir):
            raise ValueError("Download directory does not exist.")

        # Create a temporary manifest file with updated destination directories
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_manifest_file:
            with open(manifestFile) as f:
                for line in f:
                    if not line.startswith("#"):
                        pattern = r"s3:\/\/.*\*"
                        match = re.search(pattern, line)
                        if match is None:
                            raise ValueError(
                                "Could not find the bucket URL in the first line of the manifest file."
                            )
                        folder_url = match.group(0)
                        temp_manifest_file.write(
                            " sync " + folder_url + " " + downloadDir + "\n"
                        )

        cmd = [
            "s5cmd",
            "--no-sign-request",
            "--endpoint-url",
            endpoint_to_use,
            "run",
            temp_manifest_file.name,
        ]
        if quiet:
            stdout = subprocess.DEVNULL
            stderr = subprocess.DEVNULL
        else:
            stderr = subprocess.PIPE
            stdout = subprocess.PIPE

        with subprocess.Popen(
            cmd, stderr=stderr, stdout=stdout, universal_newlines=True
        ) as process:
            # Track progress using tqdm
            self._track_download_progress(total_size, downloadDir, process)

    def download_from_selection(
        self,
        downloadDir,
        dry_run=False,
        collection_id=None,
        patientId=None,
        studyInstanceUID=None,
        seriesInstanceUID=None,
    ):
        """Download the files corresponding to the selection. The filtering will be applied in sequence (but does it matter?) by first selecting the collection(s), followed by
        patient(s), study(studies) and series. If no filtering is applied, all the files will be downloaded.

        Args:
            collection_id: string or list of strings containing the values of collection_id to filter by
            patientId: string or list of strings containing the values of PatientID to filter by
            studyInstanceUID: string or list of strings containing the values of DICOM StudyInstanceUID to filter by
            seriesInstanceUID: string or list of strings containing the values of DICOM SeriesInstanceUID to filter by
            downloadDir: string containing the path to the directory to download the files to

        Returns:

        Raises:
            TypeError: If any of the parameters are not of the expected type
        """

        if collection_id is not None:
            if not isinstance(collection_id, str) and not isinstance(
                collection_id, list
            ):
                raise TypeError("collection_id must be a string or list of strings")
        if patientId is not None:
            if not isinstance(patientId, str) and not isinstance(patientId, list):
                raise TypeError("patientId must be a string or list of strings")
        if studyInstanceUID is not None:
            if not isinstance(studyInstanceUID, str) and not isinstance(
                studyInstanceUID, list
            ):
                raise TypeError("studyInstanceUID must be a string or list of strings")
        if seriesInstanceUID is not None:
            if not isinstance(seriesInstanceUID, str) and not isinstance(
                seriesInstanceUID, list
            ):
                raise TypeError("seriesInstanceUID must be a string or list of strings")

        if collection_id is not None:
            result_df = self._filter_by_collection_id(self.index, collection_id)
        else:
            result_df = self.index

        if patientId is not None:
            result_df = self._filter_by_patient_id(result_df, patientId)

        if studyInstanceUID is not None:
            result_df = self._filter_by_dicom_study_uid(result_df, studyInstanceUID)

        if seriesInstanceUID is not None:
            result_df = self._filter_by_dicom_series_uid(result_df, seriesInstanceUID)

        total_size = result_df["series_size_MB"].sum()
        logger.info(
            "Total size of files to download: " + str(float(total_size) / 1000) + "GB"
        )
        logger.info(
            "Total free space on disk: "
            + str(psutil.disk_usage(downloadDir).free / (1024 * 1024 * 1024))
            + "GB"
        )

        if dry_run:
            logger.info(
                "Dry run. Not downloading files. Rerun with dry_run=False to download the files."
            )
            return

        # Download the files
        # make temporary file to store the list of files to download
        manifest_file = os.path.join(downloadDir, "download_manifest.s5cmd")
        for index, row in result_df.iterrows():
            with open(manifest_file, "a") as f:
                f.write(
                    "sync --show-progress "
                    + row["series_aws_url"]
                    + " "
                    + downloadDir
                    + "\n"
                )
        self.download_from_manifest(manifest_file, downloadDir)
        # Delete the manifest file after download
        os.remove(manifest_file)

    def sql_query(self, sql_query):
        """Execute SQL query against the table in the index using duckdb.

        Args:
            sql_query: string containing the SQL query to execute. The table name to use in the FROM clause is 'index' (without quotes).

        Returns:
            pandas dataframe containing the results of the query

        Raises:
            any exception that duckdb.query() raises
        """

        index = self.index
        return duckdb.query(sql_query).to_df()
