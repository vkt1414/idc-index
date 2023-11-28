import os
import re
import requests
import sys
from google.cloud import bigquery
from github import Github

# Set up BigQuery client
project_id = "idc-external-025"
client = bigquery.Client(project=project_id)
owner='ImagingDataCommons'

def extract_index_version(file_path):
    with open(file_path, "r") as file:
        for line in file:
            if "def get_idc_version(self):" in line:
                return int(re.findall(r"v(\d+)", next(file))[0])

def update_index_version(file_path, latest_idc_release_version):
    with open(file_path, "r") as file:
        lines = file.readlines()

    with open(file_path, "w") as file:
        for line in lines:
            if "def get_idc_version(self):" in line:
                line = re.sub(r"v(\d+)", f"v{latest_idc_release_version}", line)
            file.write(line)

def execute_sql_query(sql_query):
    df = client.query(sql_query).to_dataframe()
    return df

def update_sql_query(file_path, current_index_version, latest_idc_release_version):
    with open(file_path, "r") as file:
        sql_query = file.read()

    if current_index_version < latest_idc_release_version:
        modified_sql_query = sql_query.replace(
            f"idc_v{current_index_version}", f"idc_v{latest_idc_release_version}"
        )

        df = execute_sql_query(modified_sql_query)
        csv_file_name = f"{os.path.basename(file_path).split('.')[0]}.csv.zip"
        df.to_csv(csv_file_name, compression='gzip', escapechar="\\")

        with open(file_path, "w") as file:
            file.write(modified_sql_query)
    else:
        raise ValueError('Current version is not less than the latest version')

    return modified_sql_query, csv_file_name

# Get latest IDC release version
view_id = "bigquery-public-data.idc_current.dicom_all_view"
view = client.get_table(view_id)
latest_idc_release_version = int(re.search(r"idc_v(\d+)", view.view_query).group(1))

current_index_version = extract_index_version('idc_index/index.py')

# Initialize the release body with information about the latest IDC release
release_body = (
    "Found newer IDC release with version "
    + str(latest_idc_release_version)
    + ".\n"
)

# List to store information for release creation
release_info_list = []
if current_index_version < latest_idc_release_version:
    # Update the index.py file with the latest IDC release version
    update_index_version('idc_index/index.py', latest_idc_release_version)
    # Iterate over all SQL query files in the 'queries/' directory
    for file_name in os.listdir("queries/"):
        if file_name.endswith(".sql"):
            file_path = os.path.join("queries/", file_name)

            modified_sql_query, csv_file_name = update_sql_query(file_path, current_index_version, latest_idc_release_version)

            # Append information for each query to the release body
            release_body += (
                "\nUpdating the index from idc_v"
                + str(current_index_version)
                + " to idc_v"
                + str(latest_idc_release_version)
                + "\nThe sql query used for generating the new csv index is \n```\n"
                + modified_sql_query
                + "\n```"
            )
            release_info_list.append((csv_file_name,))
    os.environ['create_release'] = str(True)         
    os.environ['current_index_version'] = current_index_version
    os.environ['release_body'] = release_body
    os.environ['pull_request_body'] = f'Update queries to v{latest_idc_release_version}'        
else:
    os.environ['create_release'] = str(False)            
