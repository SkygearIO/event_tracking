class Writer(object):
    def __init__(self, engine, schema, table_prefix):
        self._engine = engine
        self._schema = schema
        self._table_prefix = table_prefix
