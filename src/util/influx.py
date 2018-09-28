import datetime
import influxdb
import queue

seen = queue.Queue()


def start():
    client = influxdb.InfluxDBClient('localhost', 8086, 'admin', 'admin', 'youtube')

    while True:
        json_single = seen.get(block=True)
        client.write_points(json_single)
        print('Insert at', datetime.datetime.now())


def json(name, fields):
    return {
        'measurement': 'Channels',
        'tags': name,
        'time': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fields': fields
    }
