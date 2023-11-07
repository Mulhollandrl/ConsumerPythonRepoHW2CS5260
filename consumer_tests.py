import unittest
import argparse
from unittest.mock import patch, MagicMock
from consumer import process_request

class TestConsumerProgram(unittest.TestCase):
    @patch('boto3.Session')
    def test_process_request_create_s3(self, mock_session):
        # Mocking the AWS resources with the aptly named MagickMock
        mock_s3 = MagicMock()
        mock_dynamodb = MagicMock()
        mock_session.return_value.resource.side_effect = [mock_s3, mock_dynamodb]

        # Test data that is really simple
        request = {
            'type': {'pattern': 'create'},
            'owner': {'pattern': 'Test Owner'},
            'widgetId': '123'
        }
        # Test it with S3
        args = argparse.Namespace()
        args.storage_strategy = 's3'

        # Call the main function
        process_request(request, args)

        # Assert that the correct methods were called
        mock_s3.Bucket().put_object.assert_called_once()
        self.assertEqual(mock_dynamodb.Table().put_item.call_count, 0)


    @patch('boto3.Session')
    def test_process_request_create_dynamodb(self, mock_session):
        # Mocking the AWS resources
        mock_s3 = MagicMock()
        mock_dynamodb = MagicMock()
        mock_session.return_value.resource.side_effect = [mock_s3, mock_dynamodb]

        # Test data that is really simple
        request = {
            'type': {'pattern': 'create'},
            'owner': {'pattern': 'Test Owner'},
            'widgetId': '123'
        }
        # Test with dynamodb
        args = argparse.Namespace()
        args.storage_strategy = 'dynamodb'

        # Call the main function
        process_request(request, args)

        # Assert that the correct methods were called
        mock_dynamodb.Table().put_item.assert_called_once()
        self.assertEqual(mock_s3.Bucket().put_object.call_count, 0)

if __name__ == '__main__':
    unittest.main()