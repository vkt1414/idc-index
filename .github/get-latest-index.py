import os
import re
import requests
import sys
from google.cloud import bigquery

# Set up BigQuery client
project_id = "vamsithiriveedhi"
client = bigquery.Client(project=project_id)


def extract_index_version(file_path):
    with open(file_path, "r") as file:
        for line in file:
            if "def get_idc_version(self):" in line:
                return int(re.findall(r"v(\d+)", next(file))[0])

def update_index_version(file_path, latest_idc_release_version):
    # Open the file in read mode and read all lines
    with open(file_path, "r") as file:
        lines = file.readlines()

    # Open the file in write mode
    with open(file_path, "w") as file:
        # Iterate over each line
        for i in range(len(lines)):
            # If the line contains the string "def get_idc_version(self):"
            if "def get_idc_version(self):" in lines[i]:
                # Replace the version number in the next line
                lines[i+1] = re.sub(r"v(\d+)", f"v{latest_idc_release_version}", lines[i+1])
            # Write the line to the file
            file.write(lines[i])


def execute_sql_query(sql_query):
    df = client.query(sql_query).to_dataframe()
    return df

def create_csv_zip_from_query(query, csv_file_name):
    df = execute_sql_query(query)
    df.to_csv(csv_file_name, compression='gzip', escapechar="\\")

def update_sql_query(file_path, current_index_version, latest_idc_release_version):
    with open(file_path, "r") as file:
        sql_query = file.read()

    modified_sql_query = sql_query.replace(
        f"idc_v{current_index_version}", f"idc_v{latest_idc_release_version}"
    )
    with open(file_path, "w") as file:
        file.write(modified_sql_query)

    #create csv zips while updating sql queries
    csv_file_name = f"{os.path.basename(file_path).split('.')[0]}.csv.zip"
    create_csv_zip_from_query(modified_sql_query,csv_file_name)

    return modified_sql_query,csv_file_name

# Get latest IDC release version
view_id = "bigquery-public-data.idc_current.dicom_all_view"
view = client.get_table(view_id)
latest_idc_release_version = int(re.search(r"idc_v(\d+)", view.view_query).group(1))

current_index_version = extract_index_version('idc_index/index.py')

if current_index_version < latest_idc_release_version:
    # Update the index.py file with the latest IDC release version
    update_index_version('idc_index/index.py', latest_idc_release_version)
    # Iterate over all SQL query files in the 'queries/' directory
    for file_name in os.listdir("queries/"):
        if file_name.endswith(".sql"):
            file_path = os.path.join("queries/", file_name)

            modified_sql_query, csv_file_name = update_sql_query(file_path, current_index_version, latest_idc_release_version)

    os.environ['create_release'] = str(True)         
    os.environ['current_index_version'] = str(current_index_version)
    os.environ['pull_request_body'] = f'Update queries to v{latest_idc_release_version}'        
else:
    os.environ['create_release'] = str(False)
