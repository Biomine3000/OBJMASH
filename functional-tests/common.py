# -*- coding: utf-8 -*-
from uuid import uuid4

from obj_mash import BusinessObject, reply_for_object

from robot.api import logger

__all__ = ["make_subscription_object",
           "make_legacy_subscription_object",
           "make_object_with_natures",
           "make_event",
           "make_text_object",
           "make_application_object",
           "parse_subscriptions",
           "object_should_have_key",
           "object_should_have_key_with_value"]

# Subscription
def make_subscription_object(subscriptions=[]):
    return BusinessObject({'event': 'routing/subscribe',
                           'subscriptions': subscriptions}, None)

# Events
def make_event(event, natures=[]):
    return BusinessObject({'event': event, 'natures': natures}, None)

# Objects with payload
def make_text_object(text, natures=[]):
    payload = text.encode('utf-8')
    metadata = {
        'size': len(payload),
        'type': "text/plain; charset=UTF-8",
        'natures': natures
    }
    return BusinessObject(metadata, bytes(payload))

def make_application_object(payload):
    metadata_dict = {
        'size': len(payload),
        'type': "application/octet-stream"
    }
    return BusinessObject(metadata_dict, bytearray(payload, encoding='utf-8'))

# Natures
def make_object_with_natures(natures):
    return BusinessObject({'natures': natures}, None)

def parse_subscriptions(expr):
    return [item.strip() for item in expr.split(',')]

# Common test keywords
def object_should_have_key(obj, key):
    if not obj.metadata.has_key(key):
        logger.info("Object metadata: " + str(obj.metadata))
        raise Exception("Object should have had metadata key " + key)

def object_should_have_key_with_value(obj, key, value):
    object_should_have_key(obj, key)

    if obj.metadata[key] != value:
        logger.info("Object metadata: " + str(obj.metadata))
        raise Exception("Object should have had metadata key " + str(key) +
                        " with value " + str(value))

# Legacy support
def make_legacy_subscription_object():
    metadata = {
        'event': 'routing/subscribe',
        'receive-mode': 'all',
        'types': 'all'
        }
    result = BusinessObject(metadata, None)
    logger.info("Subscription object: " + str(result.metadata))
    return result

def make_legacy_no_echo_subscription_object():
    result = make_legacy_subscription_object()
    subscription.metadata['receive-mode'] = 'no_echo'
    logger.info("Subscription object: " + str(result.metadata))
    return result
