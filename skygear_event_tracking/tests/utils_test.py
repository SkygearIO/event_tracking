import unittest
from ..utils import sanitize_for_db


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
