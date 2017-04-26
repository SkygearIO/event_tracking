import re
import datetime
import uuid


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
            elif isinstance(value, int):
                self.attributes[sanitized_key] = float(value)
            elif isinstance(value, float):
                self.attributes[sanitized_key] = value
            elif isinstance(value, bool):
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
