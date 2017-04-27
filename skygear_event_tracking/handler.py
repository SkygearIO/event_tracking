from skygear.utils.db import _get_engine
from sqlalchemy import create_engine
import os
import posixpath
import skygear
import json
import logging
from .writer import Writer
from .utils import EventTrackingRequest


logger = logging.getLogger(__name__)


class Handler(object):
    def __init__(self, writer):
        self._writer = writer

    def __call__(self, request):
        # extract useful http headers
        ips = request.headers.get('x-forwarded-for')

        # parse body
        bytes = request.get_data()
        json_str = bytes.decode('utf-8')
        parsed = json.loads(json_str)

        events = parsed['events']
        event_tracking_request = EventTrackingRequest(
            http_header_ips=ips,
            json_events=events,
        )
        logger.debug('event_tracking_request: %s', event_tracking_request)
        self._writer.process_request(event_tracking_request)
        return skygear.Response(status=200)


def register_handler(
    endpoint_mount_path='/skygear_event_tracking',
    db_table_prefix='et_',
    db_schema=None,
    db_connection_uri=None,
):
    if db_connection_uri is None:
        engine = _get_engine()
    else:
        engine = create_engine(db_connection_uri)

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
