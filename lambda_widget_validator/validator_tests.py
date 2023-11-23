import unittest
import json
from widget_validator import lambda_handler, validate_request

class TestLambdaValidator(unittest.TestCase):
    def setUp(self):
        with open('test1.json', 'r') as f:
            self.test1 = json.load(f)
        with open('test2.json', 'r') as f:
            self.test2 = json.load(f)
        with open('test3.json', 'r') as f:
            self.test3 = json.load(f)

    def test_validate_request(self):
        self.assertTrue(validate_request(self.test1))
        self.assertFalse(validate_request(self.test2))
        self.assertFalse(validate_request(self.test3))
        
class TestLambdaHandler(unittest.TestCase):
    def setUp(self):
        with open('test1.json', 'r') as f:
            self.test1 = {'body': json.dumps(json.load(f))}
        with open('test2.json', 'r') as f:
            self.test2 = {'body': json.dumps(json.load(f))}
        with open('test3.json', 'r') as f:
            self.test3 = {'body': json.dumps(json.load(f))}

    def test_lambda_handler(self):
        self.assertEqual(lambda_handler(self.test1, None)['statusCode'], 200)
        self.assertEqual(lambda_handler(self.test2, None)['statusCode'], 400)
        self.assertEqual(lambda_handler(self.test3, None)['statusCode'], 400)

if __name__ == '__main__':
    unittest.main()
