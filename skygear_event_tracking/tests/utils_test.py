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
        expected = parse_rfc3339(input_)
        self.assertEqual(2017, expected.year)
        self.assertEqual(1, expected.month)
        self.assertEqual(23, expected.day)
        self.assertEqual(1, expected.hour)
        self.assertEqual(23, expected.minute)
        self.assertEqual(45, expected.second)
        self.assertEqual(6789, expected.microsecond)

        # it should be naive datetime
        self.assertEqual(None, expected.tzinfo)
