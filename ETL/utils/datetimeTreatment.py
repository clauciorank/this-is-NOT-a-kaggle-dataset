from datetime import datetime


def parse_date(date_string):
    formats = ['%Y-%m-%d', '%d-%m-%Y', '%m-%d-%Y', '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ']
    for fmt in formats:
        try:
            date = datetime.strptime(date_string, fmt)
            return date
        except ValueError:
            pass
    raise ValueError('Invalid date format')


