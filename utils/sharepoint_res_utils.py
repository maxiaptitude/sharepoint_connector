import pandas as pd
import json
from utils.general_utils import flatten_json

# Get sites data in a flatten pandas dataframe
def flatten_sites_metadata_to_pandas(json_data):
    flattened_sites_data = [flatten_json(site_element, 'site_') for site_element in json_data['value']]
    # Return data as pandas dataframe
    return pd.DataFrame(flattened_sites_data)