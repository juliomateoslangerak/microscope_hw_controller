from rx import Observable
import redis


def _write_to_db(db, key, value):
    # db.set(key, value) # TODO: verify that we don't need an atomic transaction
    print('Writing {} to {} in {}'.format(value, key, db))


def _read_from_db(db, key):
    db.get(key)


def _write_to_controller(key, value):
    print('Writing {} to {}'.format(value, key))


def _parse_key_value(s):
    return s.split('=')



def main():
    # Connect to the redis database to store the state
    redis_db = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True, encoding='utf-8')

    source = Observable.from_(['channel1:exp_time=0.123', 'channel2:ex_power=80.0']).publish()

    mapped = source.map(lambda s: _parse_key_value(s))

    mapped.subscribe(lambda x: _write_to_controller(x[0], x[1]))
    mapped.subscribe(lambda x: _write_to_db('Redis', x[0], x[1]))

    source.connect()


if __name__=='__main__':
    main()