import msal
import yaml
import json
import requests
import os
import pandas as pd

# Import utils libraries from ../utils folder
from utils.databricks_utils import get_secret_variables
from utils.yaml_utils import get_config_variables_from_file
from utils.sharepoint_res_utils import flatten_sites_metadata_to_pandas
# Create class
class Sharepoint_connector_API:

    def __init__(self, connector = None, config_file_path=None):

        # Get config variables from a specific connector.
        ## At the moment only databricks and YAML files are included as methods.
        # Credentials created in azure portal (get them from Entra ID. Register a new app if needed(more details in readme.md))



        # Config variables from Databricks   
        if connector.lower() == 'databricks':
            self.client_id, self.client_secret, self.authority , self.scope = get_secret_variables()
            print('fetched variables from script')
        # Config variables from YAML    
        elif connector.lower() == 'yaml':
            # Get config variables from file or else define as None
            self.config = load_config(config_file_path)
            self.client_id, self.client_secret, self.authority , self.scope = get_config_variables_from_file()
        else:
##############################################################################
########################## START UNCOMMENT BEFORE GIT ########################
##############################################################################

            self.client_id, self.client_secret, self.authority , self.scope = None

##############################################################################
########################## END UNCOMMENT BEFORE GIT ##########################
##############################################################################



    # Functions to support the init function

    # Create function to authenticate to sharepoint using authenticate token
    def get_authenticate_token(self, client_id, client_secret, authority, scope):
        # Getting access token
        client = msal.ConfidentialClientApplication(client_id, authority = authority, client_credential = client_secret)

        # Try to lookup an access token in cache
        token_cache = client.acquire_token_silent(scope, account = None)
        # Assign token to access token for login
        if token_cache:
            access_token = 'Bearer ' + token_cache['access_token']
            print('Access token fetched from Cache')
        else:
            token = client.acquire_token_for_client(scopes = scope)
            access_token = 'Bearer ' + token['access_token']
            print('Access token created using Azure AD')

        return access_token

    def get_graph_response(self, url, access_token):

        # Define Header
        headers = {
            'Authorization': access_token
        } 

        # Return get request using the access token
        return requests.get(url = url, headers = headers)

    # Function to convert the request response as a variable to a pandas dataframe. (this can be changed to spark if needed)
    def flatten_response_data_from_request(response):
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Convert the JSON response to a DataFrame
            return flatten_sharepoint_data(response.json())
        else:
            # Print an error message and return None
            print(f"Error: {response.status_code} - {response.text}")
            return None
    # Function to connect to desired folder (change in config file)
    def connect_to_folder(self, folder_name):
        return self.folder

    # Function to download files to Sharepoint
    def download_file(self, file_name, folder_name):
        return self.folder.get_file(file_name)

    # Function to upload files to Sharepoint
    def upload_file(self, File, file_name, folder_name):
        return self.folder.upload_file(File, file_name)

    # Function to delete files from Sharepoint
    def delete_file(self, File, file_name, folder_name):
        return self.folder.delete_file(File, file_name)

    def get_sharepoint_lists(self, site_id, access_token):
        # Construct the URL to fetch lists
        lists_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"

        # Define the headers with the access token
        headers = {
            'Authorization': access_token,
            'Content-Type': 'application/json',
        }

        try:
            # Make a GET request to fetch lists
            response = requests.get(lists_url, headers=headers)

            # Check if the request was successful (HTTP status code 200)
            if response.status_code == 200:
                # Parse the JSON response
                lists_data = response.json()

                # Extract and print list names and IDs
                for sharepoint_list in lists_data['value']:
                    list_name = sharepoint_list['name']
                    list_id = sharepoint_list['id']
                    print(f"List Name: {list_name}, List ID: {list_id}")

            else:
                # Print the error message if the request was not successful
                print(f"Error: {response.status_code} - {response.text}")

        except Exception as e:
            print(f"An error occurred: {e}")
    # run function to be created accordingly
    def run(self):
        access_token = self.get_authenticate_token(self.client_id, self.client_secret, self.authority, self.scope)
        site = 'file_system'
        site_url = f'https://xbpmf.sharepoint.com/sites/{site}/'
        url = f'https://graph.microsoft.com/v1.0/sites'
        test = self.get_graph_response(url, access_token)
        # Get df with all metadata from sites
        df = flatten_sites_metadata_to_pandas(test.json())

        ### TO DELETE AFTER TESTINGS
        df.to_csv('test.csv')
        print('file saved')
        ###########


### Testing running the class above with desire functions 
sp_connector = Sharepoint_connector_API('test_variables', None)
sharepoint_response = sp_connector.run()
print(sharepoint_response)