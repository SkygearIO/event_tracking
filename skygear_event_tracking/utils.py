import re
import datetime


def sanitize_for_db(some_str):
    '''
    Sanitize a string as such it can be used as table name or column name
    '''
    a = re.sub(r'[^a-zA-Z0-9]', '_', some_str)
    b = re.sub(r'_+', '_', a)
    c = b.strip('_')
    d = c.lower()
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
    except:
        return None
