import msal
import yaml
import json
import requests
import os
from utils.databricks_utils import get_secret_variables
from utils.yaml_utils import get_config_variables_from_file

class Sharepoint_connector_API:

    def __init__(self, connector = None, config_file_path=None):
        # Get config variables from a specific connector.
        ## At the moment only databricks and YAML files are included as methods.
        # Credentials created in azure portal (get them from Entra ID. Register a new app if needed)
        self.client_id = ''
        self.client_secret = '' 
        self.authority = 'https://login.microsoft.com/{tenant-id}'
        self.scope = ['https://graph.microsoft.com/.default']
        # Config variables from Databricks   
        if connector.lower() == 'databricks':
            #self.client_id, self.client_secret, self.authority , self.scope = get_databricks_secrets()
            print('fetched variables from script')
        # Config variables from YAML    
        elif connector.lower() == 'yaml':
            # Get config variables from file or else define as None
            self.config = load_config(config_file_path)
            self.client_id, self.client_secret, self.authority , self.scope = get_config_variables_from_file()
        else:
            self.client_id, self.client_secret, self.authority , self.scope = None
        


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

    # Function to connect to desired folder (change in config file)
    def connect_to_folder(self, folder_name):
        self.auth_site = authenticate()
        self.folder = auth_site.Folder(folder_name)
        return self.folder

    # Function to download files to Sharepoint
    def download_file(self, file_name, folder_name):
        self.folder = connect_to_folder(folder_name)
        return self.folder.get_file(file_name)

    # Function to upload files to Sharepoint
    def upload_file(self, File, file_name, folder_name):
        self.folder = connect_to_folder(folder_name)
        return self.folder.upload_file(File, file_name)

    # Function to delete files from Sharepoint
    def delete_file(self, File, file_name, folder_name):
        self.folder = connect_to_folder(folder_name)
        return self.folder.delete_file(File, file_name)
# Test function

    def print_filepath(self, data):
        self.data = data + '00'
        return self.data

    # run function to be created accordingly
    def run(self):
        access_token = self.get_authenticate_token(self.client_id, self.client_secret, self.authority, self.scope)
        site_url = 'https://xbpmf.sharepoint.com/sites/file_system/'
        url = f'https://graph.microsoft.com/v1.0/sites?url={site_url}'
        test = self.get_graph_response(url, access_token)
        print(json.dumps(test.json(), indent = 2))
        ###########


### Testing running the class above with desire functions 
sp_connector = Sharepoint_connector_API('databricks', None)
ss = sp_connector.run()
print(ss)