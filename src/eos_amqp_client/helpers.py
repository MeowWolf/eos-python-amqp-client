import os
import json
from pprint import pformat
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


def routing_key_to_address(routing_key):
    address = routing_key.replace('.', '/')
    return f'/{address}'


def args_to_payload(args):
    converted_args = list(map(convert_bool_to_ints, args))
    return json.dumps(converted_args)


def payload_to_args(payload_string):
    try:
        payload = json.loads(payload_string)
    except:
        payload = payload_string

    converted_payload = None

    if isinstance(payload, dict):
        converted_payload = json.dumps(payload)
    elif isinstance(payload, list):
        # convert dictionaries to json strings
        converted_dicts = [json.dumps(item) if isinstance(
            item, dict) else item for item in payload]

        # convert bools to ints
        converted_payload = list(map(convert_bool_to_ints, converted_dicts))
    else:
        converted_payload = convert_bool_to_ints(payload)
    return converted_payload


def get_config_value_list(input, delimiter=','):
    return [item.strip() for item in input.split(delimiter)]


# list of lists that maps routing keys to addresses
# specified in config
def get_routing_key_to_address_list(input, delimiter=":"):
    list = get_config_value_list(input)

    def parse_item(item):
        if len(item) == 2:
            return [item[0].strip(), item[1].strip()]
        elif len(item) == 1 and item[0]:
            return [item[0].strip(), routing_key_to_address(item[0].strip())]
        else:
            log.error('Wrong format specified for routing key list')
            return None
    return [parse_item(item.split(delimiter)) for item in list]


# just the routing keys
def get_incoming_routing_key_list(routing_key_to_address_list):
    return [item[0] for item in routing_key_to_address_list if item != None]


def get_address_by_routing_key(routing_key, routing_key_to_address_list):
    match = [item for item in routing_key_to_address_list if item !=
             None and item[0] == routing_key]
    if len(match) > 0 and len(match[0]) > 0:
        return match[0][1]

    # we didn't get an exact match from config
    # but routing key pattern still matched
    # do the normal key -> address conversion
    return routing_key_to_address(routing_key)
