import yaml
import os 
# Functions to support the library
# Load config from YAML
def load_config():
    # Get the absolute path to the config.yml file
    script_directory = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_directory, 'config.yml')
    with open(config_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config

# Extract config variables from file
def get_config_variables_from_file(config):
    client_secret = config['share_point']['client_secret']
    print(client_secret)
    client_id = config['share_point']['client_id']
    authority = config['share_point']['authority']
    print(authority)
    scope = config['share_point']['scope']
    return client_secret, client_id, authority, scope
