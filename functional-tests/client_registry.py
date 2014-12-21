# -*- coding: utf-8 -*-
import json

from os import environ as env
from uuid import uuid4

from robot.api import logger

from obj_mash import BusinessObject

__all__ = ["should_reply_with_correct_object",
           "make_list_request",
           "make_join_request"]


def make_join_request():
    return BusinessObject({'event': 'services/request',
                           'name': 'clients',
                           'request': 'join',
                           'client': "ROBOT",
                           'user': env['USER']}, None)

def make_list_request():
    return BusinessObject({'event': 'services/request',
                           'name': 'clients',
                           'request': 'list'}, None)

def should_reply_with_correct_object(routing_id, request, reply):
    logger.info("Reply metadata: " + str(reply.metadata))
    payload_text = reply.payload.decode('utf-8')
    payload = json.loads(payload_text)

    if not isinstance(payload, dict):
        raise Exception("payload is not a dict")

    d = None
    for k, v in payload.iteritems():
        if 'routing-id' not in v:
            raise Exception("attribute 'routing-id' not in client value in client dict")
        if v['routing-id'] == routing_id:
            d = v
            break

    if d is None:
        raise Exception('Client not present in returned client listing')

    if request.metadata['client'] != d['client']:
        raise Exception("attribute 'client' not equal")

    if request.metadata['user'] != d['user']:
        raise Exception("attribute 'user' not equal")
