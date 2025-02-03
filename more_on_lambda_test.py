import unittest
from more_on_lambda import *
from unittest.mock import patch, Mock


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.logger = create_logger()
        self.access, self.secret, self.bucket = load_dotEnv()
        self.mock_event = {
            "queryStringParameters": {
                "name": "Hussein El Saadi",
                "email": "huseinelsaadi@gmail.com",
                "phone": "+96171582689"
            }
        }

    @patch('boto3.client')
    def test_canLoadDotEnv(self, mock_client):
        access, secret, bucket = load_dotEnv()
        self.assertIsNotNone(access, 'Access is not null')
        self.assertIsNotNone(secret, 'Secret is not null')
        self.assertIsNotNone(bucket, 'bucket is not null')

    @patch('boto3.client')
    def test_canCreateS3Client(self, mock_client):
        mock_client.return_value = Mock()
        s3_client = create_s3Client(self.logger)
        self.assertIsNotNone(s3_client, 'S3 client should be created')

    def test_canGetData(self):
        data = get_data(self.mock_event)
        self.assertIsInstance(data, dict, "Returned data should be a dictionary")
        self.assertEqual(data["name"], "Hussein El Saadi", "Name should match the input")
        self.assertEqual(data["email"], "huseinelsaadi@gmail.com", "Email should match the input")
        self.assertEqual(data["phone"], "+96171582689", "Phone should match the input")

    def test_canCreateCSV(self):
        data = get_data(self.mock_event)
        csv_data = create_csv(data, self.logger)
        self.assertIsNotNone(csv_data, 'csv has been created')

    @patch('boto3.client')
    def test_canPutObject(self, mock_client):
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        mock_s3.return_value = None
        mock_s3.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        data = get_data(self.mock_event)
        csv_data = create_csv(data, self.logger)
        key = put_object(mock_s3, self.bucket, data, csv_data, self.logger)
        self.assertIsNotNone(key, 'key should not be null')

    @patch('boto3.client')
    def test_canGetObject(self, mock_client):
        mock_s3 = Mock()
        mock_client.return_value = mock_s3
        mock_s3.create_bucket.return_value = None
        mock_s3.put_object.return_value = {'ResponseMetadata': {'HTTPStatusCode': 200}}
        mock_s3.get_object.return_value = {'Body': Mock(
            read=Mock(return_value=b'name,email,phone\nHussein El Saadi,huseinelsaadi@gmail.com,+96171582689\n'))}
        data = get_data(self.mock_event)
        csv_data = create_csv(data, self.logger)
        key = put_object(mock_s3, self.bucket, data, csv_data, self.logger)
        obj = get_object(mock_s3, self.bucket, key, self.logger)
        self.assertIsNotNone(obj, 'object is not null')


if __name__ == '__main__':
    unittest.main()
