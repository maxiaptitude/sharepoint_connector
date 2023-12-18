import pandas as pd
import json
from utils.general_utils import flatten_json
import requests

# Get sites data in a flatten pandas dataframe (use for nested JSONS)
def flatten_sites_metadata_to_pandas(json_data):
    flattened_sites_data = [flatten_json(site_element, 'site_') for site_element in json_data['value']]
    # Return data as pandas dataframe
    return pd.DataFrame(flattened_sites_data)

# Upload files to specific site and folder in Sharepoint
def upload_file_to_sharepoint(access_token, site_id, filename,folder_path = ''):

    # Define upload request
    upload_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root:{folder_path}/{filename}:/content"
    
    # Open file to write
    with open(filename, 'rb') as file:
        # Define header for request
        headers = {
            'Authorization': access_token,
            'Content-Type': 'application/octet-stream',
        }
        # PUT request to upload file
        response = requests.put(upload_url, headers=headers, data=file)
        
        if response.status_code == 201 or response.status_code == 200:
            print(f"File '{filename}' uploaded successfully.")
        else:
            print(f"Failed to upload {filename}. please check Traceback below: {response.text}")
