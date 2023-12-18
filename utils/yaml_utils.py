import yaml

# Functions to support the library
# Load config from YAML
def load_config(config_file_path):
    with open(config_file_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config

# Extract config variables from file
def get_config_variables_from_file():
    client_secret = config['share_point']['client_secret']
    client_id = config['share_point']['client_id']
    authority = config['share_point']['authority']
    scope = config['share_point']['scope']
    return client_secret, client_id, authority, scope
