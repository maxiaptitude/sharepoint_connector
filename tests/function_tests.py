import unittest
from unittest.mock import patch, mock_open, Mock
from sharepoint_connector import SharepointConnectorAPI


class TestSharepointConnectorAPI(unittest.TestCase):

    # Create an instance of SharepointConnectorAPI for testing purposes
    def init_api(self):
        self.api = SharepointConnectorAPI(config_file_path='test_config.yml')

    # Testing load_config function with predefined YAML content
    def test_load_config(self):
        with patch('builtins.open', mock_open(read_data='share_point:\n  user: test_user\n  password: test_password\n  url: test_url\n  site: test_site\n  folder: test_folder\n  file: test_file')):
            config = self.api.load_config('test_config.yml')
        self.assertEqual(config['share_point']['user'], 'test_user')
        self.assertEqual(config['share_point']['password'], 'test_password')
        self.assertEqual(config['share_point']['url'], 'test_url')
        self.assertEqual(config['share_point']['site'], 'test_site')
        self.assertEqual(config['share_point']['folder'], 'test_folder')
        self.assertEqual(config['share_point']['file'], 'test_file')

    # Testing get_config_variables function with predifined YAML content
    def test_get_config_variables(self):

        # Sample config for testing
        self.api.config = {'share_point': {'user': 'test_user', 'password': 'test_password',
                                           'url': 'test_url', 'site': 'test_site', 'folder': 'test_folder', 'file': 'test_file'}}

        # Calling function to test
        self.api.get_config_variables()

        # Check if variables are set correctly
        self.assertEqual(self.api.username, 'test_user')
        self.assertEqual(self.api.password, 'test_password')
        self.assertEqual(self.api.sharepoint_url, 'test_url')
        self.assertEqual(self.api.sharepoint_site, 'test_site')
        self.assertEqual(self.api.sharepoint_folder, 'test_folder')
        self.assertEqual(self.api.sharepoint_file, 'test_file')

    # Testing delete_files function with predifined YAML content
    def test_delete_file(self):

        # Set up mock objects
        mock_folder = Mock()
        mock_delete_file = Mock(return_value=True)
        mock_folder.delete_file = mock_delete_file
        self.api.connect_to_folder = Mock(return_value=mock_folder)

        # Calling function to test
        result = self.api.delete_file(
            File='test_file.txt', file_name='test_file.txt', folder_name='test_folder')

        # Check if the folder was connected
        self.api.connect_to_folder.assert_called_once_with('test_folder')
        # Check that the delete_file method was called
        mock_delete_file.assert_called_once_with(
            'test_file.txt', 'test_file.txt')

        # Check results
        self.assertTrue(result)

## Add more tests for the functions below
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# Run the script when is executed
if __name__ == '__main__':
    unittest.main()
