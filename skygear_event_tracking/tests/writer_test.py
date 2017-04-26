import datetime
import unittest
from sqlalchemy import (
    Column,
    MetaData,
    Table,
)
from sqlalchemy.dialects.postgresql import (
    DOUBLE_PRECISION,
    TEXT,
)
from ..writer import compute_columns_to_add, Writer


class WriterTest(unittest.TestCase):
    def test_compute_columns_to_add(self):
        # should return all columns if table is None
        input_ = {
            'some_str': 'some_str',
            'some_float': 2.5,
            'some_bool': True,
            'some_datetime': datetime.datetime.utcnow(),
        }
        actual = compute_columns_to_add(None, input_)
        self._assert_columns(
            ['some_str', 'some_float', 'some_bool', 'some_datetime'],
            actual,
        )

        # should return new columns
        input_table = self._make_table(
            'some_table',
            Column('some_str', TEXT),
            Column('some_float', DOUBLE_PRECISION),
        )
        actual = compute_columns_to_add(input_table, input_)
        self._assert_columns(
            ['some_bool', 'some_datetime'],
            actual,
        )

    def test_compute_quantified_table_name(self):
        input_ = 'posted_an_item_for_sale'
        writer = Writer(engine=None, schema='s', table_prefix='et_')
        actual = writer._compute_quantified_table_name(input_)
        self.assertEqual('s.et_posted_an_item_for_sale', actual)

    def _make_table(self, table_name, *cols):
        metadata = MetaData()
        return Table(table_name, metadata, *cols)

    def _assert_columns(self, expected_col_names, actual):
        self.assertEqual(len(expected_col_names), len(actual))
        actual_col_names = []
        for c in actual:
            actual_col_names.append(c.name)
        set1 = set(expected_col_names)
        set2 = set(actual_col_names)
        self.assertEqual(set1, set2)
