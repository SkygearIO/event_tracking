import unittest
import datetime

from ..client import Client


class ClientTest(unittest.TestCase):
    def _make_dummy_client(self):
        return Client('http://localhost:3000/', upload=False)

    def test_sanitize_attributes(self):
        input_ = None
        client = self._make_dummy_client()
        actual = client._sanitize_attributes(None)
        self.assertEqual(actual, None)

        input_ = {
            'some_string': 'some_string',
            'some_bool': True,
            'some_int': 1,
            'some_float': 1.5,
            1: 2,
            'something_else': {},
        }
        actual = client._sanitize_attributes(input_)
        self.assertEqual(
            input_['some_string'],
            actual['some_string'],
        )
        self.assertEqual(
            input_['some_bool'],
            actual['some_bool'],
        )
        self.assertEqual(
            input_['some_int'],
            actual['some_int'],
        )
        self.assertEqual(
            input_['some_float'],
            actual['some_float'],
        )
        self.assertTrue(1 in input_)
        self.assertTrue(1 not in actual)
        self.assertTrue('something_else' in input_)
        self.assertTrue('something_else' not in actual)

    def test_serialize_event(self):
        client = self._make_dummy_client()
        sample_date = datetime.datetime(
            2017, 5, 8,
            hour=0, minute=1, second=2, microsecond=3
        )
        input_ = {
            'some_string': 'some_string',
            'some_bool': True,
            'some_int': 1,
            'some_float': 1.5,
            'some_date': sample_date,
        }
        actual = client._serialize_event(input_)
        self.assertEqual(
            input_['some_string'],
            actual['some_string'],
        )
        self.assertEqual(
            input_['some_bool'],
            actual['some_bool'],
        )
        self.assertEqual(
            input_['some_int'],
            actual['some_int'],
        )
        self.assertEqual(
            input_['some_float'],
            actual['some_float'],
        )
        self.assertEqual(
            actual['some_date']['$type'],
            'date',
        )
        self.assertEqual(
            actual['some_date']['$date'],
            '2017-05-08T00:01:02.000003Z',
        )

    def test_prepare_request_body(self):
        input_ = []
        expected = '{"events":[]}'
        client = self._make_dummy_client()
        actual = client._prepare_request_body(input_)
        self.assertEqual(actual, expected)
