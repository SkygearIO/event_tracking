import datetime
from sqlalchemy import (
    BOOLEAN,
    Column,
    TEXT,
)
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP
from alembic.migration import MigrationContext
from alembic.operations import Operations


def from_python_type_to_col_type(python_type):
    '''
    Return the corresponding col_type for given python_type
    '''
    if python_type is str:
        return TEXT
    elif python_type is bool:
        return BOOLEAN
    elif python_type is float:
        return DOUBLE_PRECISION
    elif python_type is datetime.datetime:
        return TIMESTAMP
    raise ValueError('unknown type: ' + str(python_type))


def is_equivalent_type(python_type, col_type):
    '''
    Return True is lhs python_type is equivalent to rhs col_type
    '''
    lhs = from_python_type_to_col_type(python_type)
    rhs = col_type
    return lhs is rhs


def compute_columns_to_add(table, attributes):
    '''
    Return a list of columns that should be added to the table
    '''
    db_cols = {}
    event_cols = {}

    if table is not None:
        for col_name, col in table.columns.items():
            db_cols[col_name] = type(col.type)
    for col_name, value in attributes.items():
        event_cols[col_name] = type(value)

    # verify that common columns are equivalent
    common_cols_set = set(db_cols.keys()) & set(event_cols.keys())
    for col_name in common_cols_set:
        col_type = db_cols[col_name]
        python_type = event_cols[col_name]
        eq = is_equivalent_type(python_type, col_type)
        if not eq:
            raise TypeError('"{}": {} is not equivalent to {}'.format(
                col_name,
                str(col_type),
                str(python_type),
            ))

    # compute a list columns that should be added
    output = []
    new_cols_set = set(event_cols.keys()) - set(db_cols.keys())
    for col_name in new_cols_set:
        python_type = event_cols[col_name]
        col_type = from_python_type_to_col_type(python_type)
        output.append(Column(col_name, col_type))
    return output


class Writer(object):
    def __init__(self, engine, schema, table_prefix):
        self._engine = engine
        self._schema = schema
        self._table_prefix = table_prefix

    def _make_alembic_op(self, conn):
        '''
        Return an instance Operations which is used to
        generate DDL statement
        '''
        migration_ctx = MigrationContext.configure(conn)
        op = Operations(migration_ctx)
        return op

    def _compute_quantified_table_name(self, event_norm):
        full_table_name = self._table_prefix + event_norm
        quantified_table_name = self._schema + '.' + full_table_name
        return quantified_table_name
