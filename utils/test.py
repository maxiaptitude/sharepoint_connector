import os
import json
import msal
import requests
# Read Config file
#with open(config_file_path, 'r') as config_file:
#    config = yaml.safe_load(config_file)

# Get Config variables
#username = config['share_point']['user']
#password = config['share_point']['password']
#sharepoint_url = config['share_point']['url']
#sharepoint_site = config['share_point']['site']
#sharepoint_folder = config['share_point']['folder']
#sharepoint_file = config['share_point']['file']

### Azure logins 
#username =  'Maxi@xbpmf.onmicrosoft.com'
#password = 'xbp4811028!'

# Credentials created in azure portal (get them from Entra ID. Register a new app if needed)
client_id = '28e6a33b-3653-4cad-be88-e77678e47f65'
client_secret = 'ws~8Q~sFWQpw3BYGSUVL.Twp4MeJk6-YZ1t0zb-W' 
authority = 'https://login.microsoft.com/02c3ccc0-6680-4316-934b-97d503015046'
scope = ['https://graph.microsoft.com/.default']
# Create function to authenticate to sharepoint
def get_authenticate_token(client_id, client_secret, authority, scope):
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
access_token = get_authenticate_token(client_id, client_secret, authority, scope)

site_url = 'https://xbpmf.sharepoint.com/sites/file_system/'
url = f'https://graph.microsoft.com/v1.0/sites?url={site_url}'

def get_graph_response(url, access_token):

    # Define Header
    headers = {
        'Authorization': access_token
    } 

    # Return get request using the access token
    return requests.get(url = url, headers = headers)



site_url = 'https://xbpmf.sharepoint.com/sites/file_system/'
url = f'https://graph.microsoft.com/v1.0/sites?url={site_url}'
test = get_graph_response(url, access_token)
print(json.dumps(test.json(), indent = 2))