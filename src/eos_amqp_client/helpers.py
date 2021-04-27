import json
from pprint import pformat
from typing import List
from uuid import uuid4
from .logger import create_logger
log = create_logger(__name__)


def uuid_str():
    return str(uuid4())


def pretty_format(input):
    try:
        return json.dumps(json.loads(input), indent=2)
    except:
        return pformat(input)


def convert_bool_to_ints(possible_bool):
    if possible_bool is True:
        return 1
    elif possible_bool is False:
        return 0
    else:
        return possible_bool
