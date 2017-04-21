from skygear.utils.db import _get_engine
import os
import posixpath
import skygear

from .writer import Writer


class Handler(object):
    def __init__(self, writer):
        self._writer = writer

    def __call__(self, request):
        return skygear.Response(status=250)


def register_handler(
    endpoint_mount_path='/skygear_event_tracking',
    db_table_prefix='et_',
    db_schema=None,
    db_connection_uri=None,
):
    # TODO: support db_connection_uri
    engine = _get_engine()

    if db_schema is None:
        app_name = os.environ['APP_NAME']
        db_schema = 'app_' + app_name

    writer = Writer(
        engine=engine,
        schema=db_schema,
        table_prefix=db_table_prefix,
    )

    handler = Handler(writer)

    no_slash = endpoint_mount_path.rstrip('/')
    has_slash = posixpath.join(no_slash, '')
    skygear.handler(no_slash)(handler)
    skygear.handler(has_slash)(handler)

    return handler
