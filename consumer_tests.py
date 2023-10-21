from unittest import TestCase, main
from unittest.mock import MagicMock
import consumer
from consumer import process_request, args

class TestConsumer(TestCase):
    # I did not have enough time to finish this, and was planning on reading in the JSON files included in the folder
    
    def test_process_request_create_s3(self):
        # We want to make the storage method s3
        args.storage_strategy = 's3'
        # I asked ChatGPT a good way to test this, and it suggested MagicMock which creates a mock environment. I think this was helpful
        # for the bit that I used it
        consumer.bucket3 = MagicMock()
        # Now process the request with the consumer file
        process_request()
        # ChatGPT gave me this line, but it essentially tests to see if it was put there
        consumer.bucket3.put_object.assert_called_once()

    def test_process_request_create_dynamodb(self):
        # We want to make the storage method dynamodb and use the same stuff as before
        args.storage_strategy = 'dynamodb'
        consumer.table = MagicMock()
        process_request(self.request_create)
        consumer.table.put_item.assert_called_once()

if __name__ == '__main__':
    main()