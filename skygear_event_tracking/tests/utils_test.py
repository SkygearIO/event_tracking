import unittest
import datetime
from ..utils import sanitize_for_db, parse_rfc3339


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

