import unittest
import datetime
from ..utils import (
    EventTrackingRequest,
    SingleEvent,
    parse_datetime_from_dict,
    parse_rfc3339,
    sanitize_for_db,
    compare_column_name,
)


class UtilsTest(unittest.TestCase):
    def test_compare_column_name(self):
        cases = [
            ('_id', '_ips', -1),
            ('_sent_at', '_id', 1),
            ('_id', '_id', 0),

            ('_id', 'some_str', -1),
            ('some_str', '_id', 1),
            ('some_str', 'some_str', 0),

            ('a', 'b', -1),
            ('a', 'a', 0),
            ('b', 'a', 1),
        ]
        for lhs, rhs, expected in cases:
            actual = compare_column_name(lhs, rhs)
            self.assertEqual(actual, expected)

    def test_sanitize_for_db(self):
        cases = [
            ('Click & Press', 'click_press'),
            ('100% good', '_100_good'),
            ('_use_underscore_in_event_NAME', '_use_underscore_in_event_name'),
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
            'simple_bool': True,
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

        # should handle bool
        self.assertTrue(actual.attributes['simple_bool'], True)

        # should handle date
        self.assertTrue(isinstance(
            actual.attributes['complex_date'],
            datetime.datetime
        ))

    def test_event_tracking_request_constructor(self):
        json_events = [
            {
                '_event_raw': 'Event A',
            },
            {
                '_event_raw': 'Event B',
            },
        ]
        actual = EventTrackingRequest(
            http_header_ips='0.0.0.0',
            json_events=json_events,
        )
        for single_event in actual.events:
            self.assertTrue(isinstance(single_event, SingleEvent))
            self.assertEqual('0.0.0.0', single_event.attributes['_ips'])
