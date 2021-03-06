# -*- mode: Python; indent: 4 -*-
import email.utils
import binascii
import uuid
import json
import select
import datetime
import logging
import inspect
import traceback
import time
import socket as socket_module

METADATA_MAX_SIZE = 2048
PAYLOAD_MAX_SIZE = 10485760 # 10 megabytes
MAX_OBJECT_SIZE = METADATA_MAX_SIZE + 1 + PAYLOAD_MAX_SIZE

NUL = '\x00'.encode('utf-8')

def _lg(name=None):
    if name is not None:
        return logging.getLogger(".".join([__name__, name]))
    return logging.getLogger(__name__)

def _logger(possible_logger):
    if possible_logger is None:
        return _lg(inspect.stack()[1][3])

def dt_now():
    return datetime.datetime.now()

def host_routing_id():
    return binascii.hexlify(str(uuid.getnode()).encode('utf-8')).decode('utf-8')

def hex_from_address(address):
    return binascii.hexlify("{0}-{1}".format(*address).encode('utf-8')).decode('utf-8')

def is_metadata_received(buffer, metadata_max_size=METADATA_MAX_SIZE):
    "Returns a two-tuple of (is_metadata_received, index_of_separating_nul)."
    def first_nul(buffer):
        for i in range(min(metadata_max_size, len(buffer))):
            if buffer[i] == 0:
                return i

    if len(buffer) > 0:
        first_nul_index = first_nul(buffer)
        if first_nul_index is not None:
            return True, first_nul_index
    return False, None

def parse_metadata(buffer, first_nul_index):
    return (json.loads(buffer[0:first_nul_index].decode('utf-8', 'strict')),
            buffer[first_nul_index + 1:])

def read_until_nul(socket, last_activity_timeout_secs=5, read_timeout_secs=120):
    "Reads until first nul character and returns the bytes read as a bytes object."
    started = dt_now()
    last_activity = dt_now()
    ret = bytearray()
    while len(ret) <= MAX_OBJECT_SIZE:
        now = dt_now()
        if (now - datetime.timedelta(seconds=last_activity_timeout_secs) > last_activity or
            now - datetime.timedelta(seconds=read_timeout_secs) > started):
            raise InvalidObject("Timed out reading metadata")

        char = socket.recv(1)
        if len(char) == 0 or char == NUL:
            break
        ret.extend(char)
        last_activity = now

    return bytes(ret)

def read_object(socket, last_activity_timeout_secs=5, read_timeout_secs=120, logger=None):
    logger = _logger(logger)
    started = dt_now()
    last_activity = dt_now()
    metadata_buffer = read_until_nul(socket, last_activity_timeout_secs, read_timeout_secs)

    try:
        metadata_str = metadata_buffer.decode('utf-8', 'strict')
    except Exception as e:
        logger.error(u"Cannot decode metadata buffer: {0}".format(metadata_buffer))
        raise e

    try:
        metadata = json.loads(metadata_buffer.decode('utf-8', 'strict'))
    except Exception as e:
        logger.error(u"Cannot parse metadata JSON: {0}".format(metadata_str))
        raise e

    payload_size = metadata.get('size', -1)
    if payload_size > 0:
        buffer = bytearray()
        while len(buffer) < payload_size:
            now = dt_now()
            if (now - datetime.timedelta(seconds=last_activity_timeout_secs) > last_activity or
                now - datetime.timedelta(seconds=read_timeout_secs) > started):
                raise InvalidObject("Timed out while reading payload from {0}".format(socket))

            received = socket.recv(payload_size - len(buffer))
            if len(received) > 0:
                last_activity = now
                buffer.extend(received)

        assert(len(buffer) == payload_size)
    else:
            buffer = None

    if buffer is not None:
        return BusinessObject(metadata, bytes(buffer))
    return BusinessObject(metadata, None)

def read_object_with_timeout(socket, timeout_secs=1.0):
    rlist, wlist, xlist = select.select([socket], [], [], timeout_secs)

    if len(rlist) > 0:
        return read_object(socket)

def reply_for_object(obj, socket, timeout_secs=1.0):
    """
    Waits for a reply to a sent object (connected by in-reply-to field).

    Returns the object and seconds elapsed as two-tuple (obj, secs).
    """
    started = dt_now()
    delta = datetime.timedelta(seconds=timeout_secs)
    while True:
        rlist, wlist, xlist = select.select([socket], [], [], 0.0001)

        if dt_now() > started + delta:
            return None, timeout_secs

        if len(rlist) == 0:
            continue

        reply = read_object(socket)

        if reply is None:
            raise InvalidObject
        elif reply.metadata.get('in-reply-to', None) == obj.id:
            took = dt_now() - started
            return reply, took.total_seconds()

# TODO: parameterize timeouts
def service_connection_loop(host, port, event_handler=None, state=None, logger=None):
    assert event_handler is not None
    assert state is not None
    assert logger is not None

    def close_socket(x):
        try: x.close()
        except: pass

    socket = None
    while True:
        try:
            socket = socket_module.socket(socket_module.AF_INET, socket_module.SOCK_STREAM)
            socket.connect((host, port))
            service_event_loop(socket, event_handler=event_handler, state=state, logger=logger)
        except KeyboardInterrupt as kbi:
            close_socket(socket)
            raise kbi
        except Exception as e:
            logger.error("Connection to {0}:{1} lost: {2}; sleeping 10 seconds before retry"
                         .format(host, port, traceback.format_exc()))
            close_socket(socket)
            time.sleep(10)

def service_event_loop(socket, event_handler=None, state=None, logger=None):
    assert event_handler is not None
    assert state is not None
    assert logger is not None

    reg = BusinessObject({
        'event': 'routing/subscribe',
        'subscriptions': ['@routing/*', '@services/*', '@ping', '@pong']
        }, None)
    reg.serialize(socket=socket)
    reply, time_taken = reply_for_object(reg, socket)
    logger.info("Connected and subscribed: {0} in {1} secs".format(reply.metadata, time_taken))
    own_routing_id = reply.metadata['routing-id']

    last_pong = dt_now()
    last_ping_sent = dt_now()
    output_queue = []
    while True:
        now = dt_now()
        if last_pong + datetime.timedelta(seconds=60) < now:
            if last_ping_sent + datetime.timedelta(seconds=7) < now:
                output_queue.append(BusinessObject({'event': 'ping'}, None))
                last_ping_sent = dt_now()
                logger.debug("Ping sent")

        if last_pong + datetime.timedelta(seconds=120) < now:
            raise Exception("No pong received in 60 seconds")

        rlist = [socket]
        if len(output_queue) > 0:
            wlist = [socket]
        else:
            wlist = []
        rlist, wlist, xlist = select.select(rlist, wlist, [], 1)

        if len(wlist) > 0:
            for item in output_queue:
                item.serialize(socket)
            output_queue = []
        if len(rlist) > 0:
            obj = read_object(socket)
            if obj.event == 'pong':
                last_pong = dt_now()
                logger.debug("Pong received")
            else:
                new_state, response = event_handler(obj,
                                                    own_routing_id=own_routing_id,
                                                    state=state, logger=logger)
                assert new_state is not None
                state = new_state
                if response is not None:
                    output_queue.append(response)

class InvalidObject(Exception): pass

class BusinessObject(object):
    def __init__(self, metadata_dict, payload):
        self.metadata = metadata_dict
        self.id = metadata_dict.get('id', email.utils.make_msgid())
        self.payload = payload
        self.size = metadata_dict.get('size', 0)
        self.content_type = metadata_dict.get('type', None)
        self.event = metadata_dict.get('event', None)

    def __str__(self):
        return "<{0} {1}>".format(self.__class__.__name__, self.content_type)

    def __hash__(self):
        return self.id.__hash__()

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def serialize(self, socket=None, logger=None):
        logger = _logger(logger)
        self.metadata['id'] = self.id
        self.metadata['size'] = self.size
        if self.content_type is not None:
            self.metadata['type'] = str(self.content_type)

        if socket is None:
            metadata_and_nul = bytes(json.dumps(self.metadata).encode('utf-8') + NUL)
            if self.size > 0:
                return metadata_and_nul + self.payload
            else:
                return metadata_and_nul
        else:
            buf = bytearray()
            buf.extend(json.dumps(self.metadata).encode('utf-8'))
            buf.extend(NUL)
            if self.size > 0:
                buf.extend(self.payload)
            bytes_sent = socket.send(buf)
            assert bytes_sent == len(buf)
