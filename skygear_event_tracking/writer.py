import datetime
from sqlalchemy import (
    BOOLEAN,
    Column,
    MetaData,
    TEXT,
    Table,
)
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION, TIMESTAMP
from alembic.migration import MigrationContext
from alembic.operations import Operations
import threading
import logging

logger = logging.getLogger(__name__)


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
        self._cache = threading.local()

    def _ensure_cache(self):
        logger.debug('ensure cache')
        if not hasattr(self._cache, 'metadata'):
            self._evict_reflection_cache()

    def _evict_reflection_cache(self):
        logger.debug('evicting reflection cache')
        self._cache.metadata = MetaData()
        self._cache.tables = {}

    def _make_alembic_op(self, conn):
        '''
        Return an instance Operations which is used to
        generate DDL statement
        '''
        migration_ctx = MigrationContext.configure(conn)
        op = Operations(migration_ctx)
        return op

    def _compute_prefixed_table_name(self, event_norm):
        full_table_name = self._table_prefix + event_norm
        return full_table_name

    def _compute_quantified_table_name(self, event_norm):
        full_table_name = self._compute_prefixed_table_name(event_norm)
        quantified_table_name = self._schema + '.' + full_table_name
        return quantified_table_name

    def _create_table(self, conn, op, event_norm, columns):
        prefixed_table_name = self._compute_prefixed_table_name(
            event_norm
        )
        logger.debug('create table: %s', prefixed_table_name)
        op.create_table(
            prefixed_table_name,
            *columns,
            **{
                'schema': self._schema,
            }
        )
        self._evict_reflection_cache()
        table = self._get_cached_table(conn, event_norm)
        return table

    def _add_columns(self, conn, op, event_norm, columns):
        prefixed_table_name = self._compute_prefixed_table_name(
            event_norm
        )
        logger.debug('add columns in table: %s', prefixed_table_name)
        for column in columns:
            op.add_column(
                prefixed_table_name,
                column,
                schema=self._schema,
            )
        self._evict_reflection_cache()
        table = self._get_cached_table(conn, event_norm)
        return table

    def _get_cached_table(self, conn, event_norm):
        prefixed_table_name = self._compute_prefixed_table_name(event_norm)
        quantified_table_name = self._compute_quantified_table_name(
            event_norm,
        )
        self._ensure_cache()
        if quantified_table_name not in self._cache.tables:
            logger.debug('cache miss table: %s', prefixed_table_name)
            try:
                table = Table(
                    prefixed_table_name,
                    self._cache.metadata,
                    autoload=True,
                    autoload_with=conn,
                    schema=self._schema
                )
            except NoSuchTableError:
                table = None
            self._cache.tables[quantified_table_name] = table
        else:
            logger.debug('cache hit table: %s', prefixed_table_name)
            table = self._cache.tables[quantified_table_name]
        return table

    def _insert_event(self, conn, table, event):
        logger.debug('insert table: %s', table.name)
        stmt = table.insert().values(**event.attributes)
        conn.execute(stmt)

    def _process_one_event_in_txn(self, event):
        try:
            with self._engine.begin() as conn:
                logger.debug('transaction begins')
                op = self._make_alembic_op(conn)
                table = self._get_cached_table(conn, event.event_norm)
                columns = compute_columns_to_add(table, event.attributes)
                if len(columns) > 0:
                    if table is None:
                        table = self._create_table(
                            conn,
                            op,
                            event.event_norm,
                            columns,
                        )
                    else:
                        table = self._add_columns(
                            conn,
                            op,
                            event.event_norm,
                            columns,
                        )
                self._insert_event(conn, table, event)
        except Exception:
            self._evict_reflection_cache()

    def process_request(self, event_tracking_request):
        '''
        Process each event in separated transaction.
        Cache reflection data in thread local metadata.
        The cache will be evicted if DDL is emited or exception is caught.
        '''
        for event in event_tracking_request.events:
            self._process_one_event_in_txn(event)
