from rx import Observable
import redis
import socket
import errno
import json
import time
import random

# Configuration options
MASTER_IP = '10.6.19.11'
CONTROLLER_IP = '10.61.19.90'
BROADCAST_DOMAIN = '<broadcast>'
RECEIVE_PORT = 5000
BROADCAST_PORT = 5566
NR_CHANNELS = 3

# Creating a socket to broadcast the controller changes
broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
broadcast_socket.bind(('', 0))
broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)


def _write_to_db(db, key, value):
    # db.set(key, value) # TODO: verify that we don't need an atomic transaction
    print('Writing {} to {} in {}'.format(value, key, db))


def _read_from_db(db, key):
    db.get(key)


def _pull_from_channel(channel_nr):
    while True:
        # TODO: Simulating for now
        yield '["hwc","float","ch_' + str(channel_nr) + ':exp_time",' + str(random.random()) + ']'
        time.sleep(random.randint(0, 1))


def _write_to_controller(key, value):
    print('Writing controller {} to {}'.format(value, key))


def _parse_key_value(s):
    return s.split('=')
    # return json.loads(s)


def _socket_generator(host, port, prefix_length=4):
    """Creates a generator that provides the UDP socket messages
    returns messages at each iteration.
    Each message is preceded from a datagram_length value of length prefix_length
    indicating how long is the datagram to read.

    :parameter prefix_length: indicates the length of the prefix indicating datagram length
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    except socket.error as e:
        if e.errno == errno.ECONNREFUSED:
            # TODO: handle the case where the master computer
            pass
        else:
            print('Failed to create socket. Error code: {}'.format(e.errno))
            raise

    try:
        s.bind((host, port))
    except socket.error as e:
        if e.errno == errno.ECONNREFUSED:
            # Handle the exception...
            pass
        else:
            print('Failed to bind socket. Error code: {}'.format(e.errno))
            raise

    while True:
        try:
            # Receive Datagram
            datagram_length = int(s.recvfrom(prefix_length)[0])
            datagram = s.recvfrom(datagram_length)[0]
        except socket.error as e:
            print('Socket broken. Error code: {}'.format(e.errno))
            raise
        yield datagram


def _test_socket_generator():
    while True:
        # TODO: Simulating for now
        yield '["udp_cockpit","int","ch_' + str(random.randint(0, 3)) + ':power",' + str(random.randint(0, 100)) + ']'
        # time.sleep(random.randint(5, 10))


def _broadcast_state(message, bc_socket):
    # Simulation
    print('Broadcasting type {}, key {} and value {} through {}'.format(message[1], message[2], message[3], str(bc_socket)))
    # bc_socket.sendto(message, (BROADCAST_DOMAIN, BROADCAST_PORT))



def main():
    # Connect to the redis database to store the state
    # redis_db = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True, encoding='utf-8')
    redis_db = 'Redis'  # for simulation purposes

    # Defining the UDP source
    udp_source = Observable.from_(_test_socket_generator())

    # Defining the hw controller source
    channel_source = []
    for ch in range(NR_CHANNELS):
        channel_source.append(Observable.from_(_pull_from_channel(ch)) \
                              .distinct_until_changed())  # We only want the values that change


    hwc_source = Observable.merge(channel_source)

    merged_source = Observable.merge(hwc_source, udp_source) \
                              .map(lambda s: json.loads(s)) \
                              .publish()

    hwc_sink = merged_source.filter(lambda x: x[0].startswith('udp')) \
                            .subscribe(on_next=lambda x: _write_to_controller(x[2], x[3]),
                                       on_error=lambda e: print(e))

    udp_sink = merged_source.filter(lambda x: x[0].startswith('hwc')) \
                            .subscribe(on_next=lambda x: _broadcast_state(x, broadcast_socket),
                                       on_error=lambda e: print(e))

    db_sink = merged_source.subscribe(on_next=lambda x: _write_to_db(redis_db, x[2], x[3]),
                                      on_error=lambda e: print(e))


    merged_source.connect()


if __name__=='__main__':
    main()

    # print(sum(my_generator(10)))
