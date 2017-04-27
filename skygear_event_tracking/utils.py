import re
import datetime
import uuid
from functools import cmp_to_key


# The order of elements in this list is important.
# It defines the desired sort order of preserved columns.
_PREVERSED_COLUMNS = [
    # common columns
    '_id',
    '_event_norm',
    '_event_raw',
    '_user_id',
    '_tracked_at',
    '_sent_at',
    '_received_at',
    '_ips',

    # web specific columns
    '_user_agent',
    '_page_url',
    '_page_path',
    '_page_search',
    '_page_referrer',
    '_utm_campaign',
    '_utm_channel',

    # mobile specific columns
    '_app_id',
    '_app_version',
    '_app_build_number',
    '_device_id',
    '_device_manufacturer',
    '_device_model',
    '_device_os',
    '_device_os_version',
    '_device_carrier',
    '_device_locales',
    '_device_locale',
    '_device_timezone',
]


def _list_index_of(list_, item):
    try:
        return list_.index(item)
    except ValueError:
        return -1


def _str_compare(lhs, rhs):
    if lhs == rhs:
        return 0
    if lhs < rhs:
        return -1
    return 1


def compare_column_name(lhs, rhs):
    i = _list_index_of(_PREVERSED_COLUMNS, lhs)
    j = _list_index_of(_PREVERSED_COLUMNS, rhs)
    if i < 0:
        if j < 0:
            # both of them are not preserved
            return _str_compare(lhs, rhs)
        else:
            # rhs is preserved
            return 1
    else:
        if j < 0:
            # lhs is preserved
            return -1
        else:
            # both of them are preserved
            diff = i - j
            if diff == 0:
                return 0
            if diff < 0:
                return -1
            return 1


def compare_column(lhs, rhs):
    return compare_column_name(lhs.name, rhs.name)


def sort_columns(columns):
    return sorted(columns, key=cmp_to_key(compare_column))


def sanitize_for_db(some_str):
    '''
    Sanitize a string as such it can be used as table name or column name
    '''
    a = re.sub(r'[^a-zA-Z0-9]', '_', some_str)
    b = re.sub(r'_+', '_', a)
    d = b.lower()
    if d[0:1].isdigit():
        d = '_' + d
    return d


def parse_rfc3339(some_str):
    dt = datetime.datetime.strptime(some_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    return dt


def parse_datetime_from_dict(some_dict):
    try:
        type_ = some_dict['$type']
        if type_ != 'date':
            return None
        date_str = some_dict['$date']
        return parse_rfc3339(date_str)
    except Exception:
        return None


class EventTrackingRequest(object):
    '''
    Represent a request
    It is intended to be used by writer
    '''
    def __init__(self, http_header_ips, json_events):
        self.events = []
        self.received_at = datetime.datetime.utcnow()
        for json_event in json_events:
            json_dict = json_event.copy()
            if http_header_ips:
                json_dict['_ips'] = http_header_ips
            self.events.append(SingleEvent(
                event_id=str(uuid.uuid4()),
                received_at=self.received_at,
                json_dict=json_dict,
            ))


class SingleEvent(object):
    '''
    Represent a single event
    It is intended to be constructed by the web request handler
    and used by db writer
    '''
    def __init__(self, event_id, received_at, json_dict):
        self._event_raw = json_dict['_event_raw']
        self.event_norm = sanitize_for_db(self._event_raw)

        self.attributes = {}
        for key in json_dict:
            sanitized_key = sanitize_for_db(key)
            value = json_dict[key]
            if value is None:
                continue
            if isinstance(value, str):
                self.attributes[sanitized_key] = value
            elif isinstance(value, bool):
                self.attributes[sanitized_key] = value
            elif isinstance(value, int):
                self.attributes[sanitized_key] = float(value)
            elif isinstance(value, float):
                self.attributes[sanitized_key] = value
            elif isinstance(value, dict):
                maybe_datetime = parse_datetime_from_dict(value)
                if maybe_datetime:
                    self.attributes[sanitized_key] = maybe_datetime

        # inject _event_norm
        self.attributes['_event_norm'] = self.event_norm

        # inject _id
        self.attributes['_id'] = event_id

        # inject _received_at
        self.attributes['_received_at'] = received_at
