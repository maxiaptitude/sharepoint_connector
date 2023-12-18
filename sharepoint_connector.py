import msal
import yaml
import json
import requests
import os
import pandas as pd

# Import utils libraries from ../utils folder
from utils.databricks_utils import get_secret_variables
from utils.yaml_utils import get_config_variables_from_file
from utils.sharepoint_res_utils import flatten_sites_metadata_to_pandas, upload_file_to_sharepoint
from utils.general_utils import get_targetVariable_with_sourceVariable_from_df, flatten_json
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




        self.access_token = self.get_authenticate_token(self.client_id, self.client_secret, self.authority, self.scope)
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

    # Function to download files to Sharepoint
    # Function to delete files from Sharepoint

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

        # Get sites metadata as response
        url = f'https://graph.microsoft.com/v1.0/sites'
        sites_metadata_response = self.get_graph_response(url, self.access_token)

        # Convert it to Pandas Dataframe
        df = flatten_sites_metadata_to_pandas(sites_metadata_response.json())
        ### TO DELETE AFTER TESTINGS
#        df.to_csv('test.csv')
#        print('file saved')

        # Get specific Site ID for accessing that one
        site_id = get_targetVariable_with_sourceVariable_from_df(df, 'site_name', 'file_system', 'site_id')
        site_name = get_targetVariable_with_sourceVariable_from_df(df, 'site_name', 'file_system', 'site_name')
        print(site_name)

        # Use site_id for fetching that site_response
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        target_siteid_metadata_response = self.get_graph_response(url, self.access_token)

        # Convert response to JSON
        target_siteid_metadata_json = target_siteid_metadata_response.json()
        target_siteid_metadata_json_text = json.dumps(target_siteid_metadata_json, indent=2)

        # Save JSON Metadata as file in Local & Sharepoint(tentativo)
        with open(f'{site_name}_metadata.json', 'w') as json_file:
            json.dump(target_siteid_metadata_json, json_file, indent=2)
        
        # Save file stored in Local
        upload_file_to_sharepoint(self.access_token, site_id, f'{site_name}_metadata.json',folder_path = '')

        # Convert and save as csv in Local & Sharepoint(tentativo).
        df = pd.DataFrame([flatten_json(target_siteid_metadata_json, prefix=f'site_{site_name}')])
        df.to_csv(f'{site_name}_metadata.csv')
        upload_file_to_sharepoint(self.access_token, site_id, f'{site_name}_metadata.csv' ,folder_path = '')
        ###########

### Testing running the class above with desire functions 
sp_connector = Sharepoint_connector_API('dummy_connector', None)
sharepoint_response = sp_connector.run()