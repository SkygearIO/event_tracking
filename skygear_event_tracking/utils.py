import re


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
