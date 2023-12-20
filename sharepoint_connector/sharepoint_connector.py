import msal
import yaml
import json
import requests
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import col, explode, coalesce, lit

# Import utils libraries from ../utils folder
from utils.databricks_utils import get_secret_variables
from utils.yaml_utils import *
from utils.sharepoint_res_utils import *
from utils.general_utils import *

# Create class

class Sharepoint_connector_API:

    def __init__(self, connector=None, config_file_path=None):

        # Get config variables from a specific connector.
        # At the moment only databricks and YAML files are included as methods.
        # Credentials created in azure portal (get them from Entra ID. Register a new app if needed(more details in readme.md))

        # Create Spark session
        self.spark = SparkSession.builder \
            .appName("Sharepoint_Connector_App") \
            .getOrCreate()

        # Get config variables from Databricks
        if connector.lower() == 'databricks':
            self.client_id, self.client_secret, self.authority, self.scope = get_secret_variables()
            print('fetched variables from script')

        # Get config variables from YAML
        elif connector.lower() == 'yaml':
            # Get config variables from file or else define as None
            self.config = load_config(config_file_path)
            self.client_id, self.client_secret, self.authority, self.scope = get_config_variables_from_file()
        else:
            ##############################################################################
            ########################## START UNCOMMENT BEFORE GIT ########################
            ##############################################################################

            # self.client_id, self.client_secret, self.authority , self.scope = None

            ##############################################################################
            ########################## END UNCOMMENT BEFORE GIT ##########################
            ##############################################################################

            #####################################################################################################
            ########################## START REMOVING LINES BELOW BEFORE UPLOAD TO GIT ##########################
            #####################################################################################################

            self.client_id = '28e6a33b-3653-4cad-be88-e77678e47f65'
            self.client_secret = 'ws~8Q~sFWQpw3BYGSUVL.Twp4MeJk6-YZ1t0zb-W'
            self.authority = 'https://login.microsoft.com/02c3ccc0-6680-4316-934b-97d503015046'
            self.scope = ['https://graph.microsoft.com/.default']

###################################################################################################
########################## END REMOVING LINES BELOW BEFORE UPLOAD TO GIT ##########################
###################################################################################################
        # Get access_token
        self.access_token = self.get_authenticate_token(
            self.client_id, self.client_secret, self.authority, self.scope)


    # Functions to support the init function
    # Create function to authenticate to sharepoint using authenticate token
    def get_authenticate_token(self, client_id, client_secret, authority, scope):
        # Getting access token
        client = msal.ConfidentialClientApplication(
            client_id, authority=authority, client_credential=client_secret)

        # Try to lookup an access token in cache
        token_cache = client.acquire_token_silent(scope, account=None)
        # Assign token to access token for login
        if token_cache:
            access_token = 'Bearer ' + token_cache['access_token']
            print('Access token fetched from Cache')
        else:
            token = client.acquire_token_for_client(scopes=scope)
            access_token = 'Bearer ' + token['access_token']
            print('Access token created using Azure AD')

        return access_token

    # Get response from MS Graph API
    def get_graph_response(self, url, access_token):

        # Define Header
        headers = {
            'Authorization': access_token
        }

        # Return get request using the access token
        return requests.get(url=url, headers=headers)


    # Get site metadata as pyspark df
    def get_sites_metadata(self):

        # Get sites metadata as response
        url = f'https://graph.microsoft.com/v1.0/sites'
        sites_metadata_response = self.get_graph_response(
            url, self.access_token)

        df = self.flatten_sublevel_metadata_to_spark(
            sites_metadata_response.json(), 'value')

        return df


    # Get flattend data in a flattened PySpark DataFrame (use for nested JSONS)
    def flatten_sublevel_metadata_to_spark(self, json_data, key_to_convert):
        # Create the schema by inferring data types
        schema = StructType([StructField(key, StringType(), True)
                            for key in json_data[key_to_convert][0]])

        # Create df with schema

        spark_df = self.spark.createDataFrame([], schema=schema)

        # Populate the df with data
        for row in json_data[key_to_convert]:
            values = []
            # Get values to populate df in a list
            for key in schema.fieldNames():
                value = row.get(key) if isinstance(
                    row.get(key), dict) else row.get(key)
                values.append(value)
            # concat df with values 
            spark_df = spark_df.union(
                self.spark.createDataFrame([tuple(values)], schema=schema))
        return spark_df


    # Get specific site metadata df with site Name
    def flatten_json_response_metadata_to_spark(self, json_response):
        try:
            # Read JSON string as a df
            df = self.spark.read.json(
                self.spark.sparkContext.parallelize([json_response]))
            return df

        except Exception as e:
            print(e)
            return None


    # Get metadata df using site name
    def get_metadata_dataframe_with_site_name(self, df, site_name):
        # Get sites metadata as response
        url = f'https://graph.microsoft.com/v1.0/sites'
        sites_metadata_response = self.get_graph_response(
            url, self.access_token)
        df = self.get_sites_metadata()
        site_id = get_target_variable_with_source_variable_from_pyspark_df(
            self.spark, df, 'name', site_name, 'id')
        # Use site_id for fetching that site_response
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        sites_metadata_response = self.get_graph_response(
            url, self.access_token)
        df = self.flatten_json_response_metadata_to_spark(
            sites_metadata_response.json())
        return df

    # Get all files metadata df with general info
    def get_all_files_metadata_with_site_name(self, df, site_name):
        # Get sites metadata as response
        url = f'https://graph.microsoft.com/v1.0/sites'
        sites_metadata_response = self.get_graph_response(
            url, self.access_token)
        df = self.get_sites_metadata()
        site_id = get_target_variable_with_source_variable_from_pyspark_df(
            self.spark, df, 'name', site_name, 'id')

        # Use site_id for fetching that site_response
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        site_name_metadata_response = self.get_graph_response(
            url, self.access_token)
        # Get df
        df = self.flatten_json_response_metadata_to_spark(
            site_name_metadata_response.json())

        # Get new id
        site_id = get_target_variable_with_source_variable_from_pyspark_df(
            self.spark, df, 'name', site_name, 'id')

        # Use new id to get new df
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
        site_files_metadata_response = self.get_graph_response(
            url, self.access_token)
        df = self.flatten_sublevel_metadata_to_spark(
            site_files_metadata_response.json(), 'value')

        return df


    # Get specific file metadata with site_name
    def get_specific_file_metadata_with_site_name(self, df, site_name, file_name):
        # Get sites metadata as response
        url = f'https://graph.microsoft.com/v1.0/sites'
        sites_metadata_response = self.get_graph_response(
            url, self.access_token)
        df = self.get_sites_metadata()
        site_id = get_target_variable_with_source_variable_from_pyspark_df(
            self.spark, df, 'name', site_name, 'id')

        # Use site_id for fetching that site_response
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        site_name_metadata_response = self.get_graph_response(
            url, self.access_token)
        # Get df
        df = self.flatten_json_response_metadata_to_spark(
            site_name_metadata_response.json())

        # Get new site id
        site_id = get_target_variable_with_source_variable_from_pyspark_df(
            self.spark, df, 'name', site_name, 'id')

        # Use new id to get new df
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
        site_files_metadata_response = self.get_graph_response(
            url, self.access_token)
        df = self.flatten_sublevel_metadata_to_spark(
            site_files_metadata_response.json(), 'value')
        # Get file id
        # file_ids = get_target_variable_with_source_variable_from_pyspark_df(self.spark, df, 'name', file_name, 'id')
        file_ids_dict = {row['name']: row['id'] for row in df.collect()}
        file_id = file_ids_dict.get('folder')
        # Get file metadata as response
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}"
        file_metadata_response = self.get_graph_response(
            url, self.access_token)

        # Convert to df
        df = self.flatten_json_response_metadata_to_spark(
            file_metadata_response.json())

        return df

#    Get all specific files metadata in one dataframe (Not working)
    def get_all_specific_file_metadata_with_site_name(self, df, site_name):
        # Get sites metadata as response
        url = f'https://graph.microsoft.com/v1.0/sites'
        sites_metadata_response = self.get_graph_response(
            url, self.access_token)

        df = self.get_sites_metadata()
        
        site_id = get_target_variable_with_source_variable_from_pyspark_df(
            self.spark, df, 'name', site_name, 'id')

        # Use site_id for fetching that site_response
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}"
        site_name_metadata_response = self.get_graph_response(
            url, self.access_token)
        # Get df
        df = self.flatten_json_response_metadata_to_spark(
            site_name_metadata_response.json())

        # Get new site id
        site_id = get_target_variable_with_source_variable_from_pyspark_df(
            self.spark, df, 'name', site_name, 'id')

        # Use new id to get new df
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
        site_files_metadata_response = self.get_graph_response(
            url, self.access_token)

        df = self.flatten_sublevel_metadata_to_spark(
            site_files_metadata_response.json(), 'value')

        # Get file id
        # file_ids = get_target_variable_with_source_variable_from_pyspark_df(self.spark, df, 'name', file_name, 'id')
        file_ids_dict = {row['name']: row['id'] for row in df.collect()}
        file_id = file_ids_dict.get('folder')

        # Get file metadata as response
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/items/{file_id}"
        file_metadata_response = self.get_graph_response(
            url, self.access_token)

        # Convert to df
        df = self.flatten_json_response_metadata_to_spark(
            file_metadata_response.json())
        return all_files_metadata_df

    # Test function
    def test_run(self):

        # Get sites metadata as response
        url = f'https://graph.microsoft.com/v1.0/sites'
        sites_metadata_response = self.get_graph_response(
            url, self.access_token)

        # get sites metadata df
        df = self.get_sites_metadata()

        # get site metadata df with site name
        metadata_dataframe_with_site_name = self.get_metadata_dataframe_with_site_name(
            df, 'file_system')

        # get site metadata df with site name
        files_metadata_dataframe_with_site_name = self.get_all_files_metadata_with_site_name(
            df, 'file_system')

        # Get specific file metadata df
        specific_file_metadata = self.get_specific_file_metadata_with_site_name(
            df, 'file_system', 'file_system_files_metadata.csv')

        # Get all specific file metadata df
        # all_specific_file_metadata = self.get_all_specific_file_metadata_with_site_name(df, 'file_system')
        # Get list of all files in this site

        return df, specific_file_metadata, metadata_dataframe_with_site_name, files_metadata_dataframe_with_site_name, specific_file_metadata
