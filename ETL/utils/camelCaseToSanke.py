import re


def camel_case_to_snake(string):
    name = re.sub(r'(?<!^)(?=[A-Z])', '_', string).lower()

    return name
