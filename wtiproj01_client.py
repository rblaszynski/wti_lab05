import redis
import json

r = redis.StrictRedis(host='localhost', port=6381, db=0)


def add_to_queue(name, value):
    return r.rpush(name, json.dumps(value))


def show_elements(value, start, stop):
    return r.lrange(value, start, stop)


def show_all_ratings():
    return r.lrange('ratings', 0, -1)


def delete_all():
    return r.flushdb()


def delete_elements(value, start, stop):
    return r.ltrim(value, start, stop)


def pull_whole_queue(queue_name):
    whole_queue_batch = r.lrange(queue_name, 0, -1)
    r.ltrim(queue_name, len(whole_queue_batch), -1)
    return whole_queue_batch


def get_whole_queue(queue_name):
    return r.lrange(queue_name, 0, -1)


def get_last_element(name):
    return r.lindex(name, -1)
