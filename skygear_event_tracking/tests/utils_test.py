import unittest
import datetime
from ..utils import (
    SingleEvent,
    parse_datetime_from_dict,
    parse_rfc3339,
    sanitize_for_db,
)


class UtilsTest(unittest.TestCase):
    def test_sanitize_for_db(self):
        cases = [
            ('Click & Press', 'click_press'),
            ('100% good', '_100_good'),
            ('_use_underscore_in_event_NAME', 'use_underscore_in_event_name'),
        ]
        for input_, expected in cases:
            actual = sanitize_for_db(input_)
            self.assertEqual(actual, expected)

    def test_parse_rfc3339(self):
        input_ = '2017-01-23T01:23:45.006789Z'
        actual = parse_rfc3339(input_)
        self.assertEqual(2017, actual.year)
        self.assertEqual(1, actual.month)
        self.assertEqual(23, actual.day)
        self.assertEqual(1, actual.hour)
        self.assertEqual(23, actual.minute)
        self.assertEqual(45, actual.second)
        self.assertEqual(6789, actual.microsecond)
        self.assertEqual(None, actual.tzinfo)

    def test_parse_datetime_from_dict(self):
        input_ = {
            '$type': 'date',
            '$date': '2017-01-23T01:23:45.006789Z',
        }
        actual = parse_datetime_from_dict(input_)
        self.assertEqual(2017, actual.year)
        self.assertEqual(1, actual.month)
        self.assertEqual(23, actual.day)
        self.assertEqual(1, actual.hour)
        self.assertEqual(23, actual.minute)
        self.assertEqual(45, actual.second)
        self.assertEqual(6789, actual.microsecond)
        self.assertEqual(None, actual.tzinfo)

        input_ = {}
        actual = parse_datetime_from_dict(input_)
        self.assertEqual(None, actual)

    def test_single_event_constructor(self):
        event_id = 'abc'
        received_at = datetime.datetime.utcnow()
        json_dict = {
            '_event_raw': 'Posted an item for sale',
            'simple_str': 'simple_str',
            'simple_int': 1,
            'simple_float': 1.5,
            'complex_date': {
                '$type': 'date',
                '$date': '2017-01-23T01:23:45.006789Z',
            },
        }
        actual = SingleEvent(
            event_id=event_id,
            received_at=received_at,
            json_dict=json_dict,
        )

        # should inject _event_norm
        self.assertTrue('_event_norm' in actual.attributes)
        self.assertEqual(actual.attributes['_event_norm'], actual.event_norm)

        # should inject _id
        self.assertEqual(actual.attributes['_id'], event_id)

        # should handle str
        self.assertEqual(actual.attributes['simple_str'], 'simple_str')

        # should convert int to float
        self.assertEqual(actual.attributes['simple_int'], 1.0)
        self.assertTrue(isinstance(actual.attributes['simple_int'], float))

        # should handle float
        self.assertEqual(actual.attributes['simple_float'], 1.5)

        # should handle date
        self.assertTrue(isinstance(
            actual.attributes['complex_date'],
            datetime.datetime
        ))
