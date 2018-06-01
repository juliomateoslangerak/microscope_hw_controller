from rx import Observable
import redis
import socket
import errno
import json

MASTER_IP = '10.6.19.11'
RECEIVE_PORT = 5000


def _write_to_db(db, key, value):
    # db.set(key, value) # TODO: verify that we don't need an atomic transaction
    print('Writing {} to {} in {}'.format(value, key, db))


def _read_from_db(db, key):
    db.get(key)


def _pull_from_controller():
    pass


def _write_to_controller(key, value):
    print('Writing {} to {}'.format(value, key))


def _parse_key_value(s):
    return json.loads(s)


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
            # Handle the exception...
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


def my_generator(n=10):
    # a generator that yields items instead of returning a list
    yield 'key=' + str(n)
    yield 'key=' + str(n+2)



def main():
    # Connect to the redis database to store the state
    redis_db = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True, encoding='utf-8')

    # source = Observable.from_(['channel1:exp_time=0.123', 'channel2:ex_power=80.0']).publish()
    # source = Observable.from_(my_generator(10)).publish()
    udp_source = Observable.from_(_socket_generator(MASTER_IP, RECEIVE_PORT)).publish()


    mapped = udp_source.map(lambda s: _parse_key_value(s))

    mapped.subscribe(lambda x: _write_to_controller(x[0], x[1]))
    mapped.subscribe(lambda x: _write_to_db('Redis', x[0], x[1]))

    udp_source.connect()


if __name__=='__main__':
    main()

    # print(sum(my_generator(10)))
