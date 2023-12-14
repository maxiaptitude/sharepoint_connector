from pyspark.sql import SparkSession
from shareplum import Site, Office365
from shareplum.site import Version
import yaml
import json
import os


class Sharepoint_connector_API:

    def __init__(self, config_file_path, df=None):

        self.data = df
        # Get config variables
        self.config = self.load_config(config_file_path)
        self.get_config_variables()

# Functions to support the init function
    # Load config
    def load_config(self, config_file_path):
        with open(config_file_path, 'r') as config_file:
            config = yaml.safe_load(config_file)
        return config

    # Extract config variables
    def get_config_variables(self):
        self.username = self.config['share_point']['user']
        self.password = self.config['share_point']['password']
        self.sharepoint_url = self.config['share_point']['url']
        self.sharepoint_site = self.config['share_point']['site']
        self.sharepoint_folder = self.config['share_point']['folder']
        self.sharepoint_file = self.config['share_point']['file']

    # Create function to authenticate to sharepoint
    def authenticate(self):
        self.auth_cookie = Office365(
            sharepoint_url, username=username, password=password).GetCookies()
        self.site = Site(sharepoint_site, version=Version.v365,
                         authcookie=auth_cookie)

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
        print(f"Username: {self.username}")
        print(f"Password: {self.password}")
        print(f"SharePoint URL: {self.sharepoint_url}")
        print(f"SharePoint Site: {self.sharepoint_site}")
        print(f"SharePoint Folder: {self.sharepoint_folder}")
        print(f"SharePoint File: {self.sharepoint_file}")
        # sharepoint_file  = download_file(self, sharepoint_file, sharepoint_folder)
        data = self.print_filepath('data')
        print(data)
        return data
