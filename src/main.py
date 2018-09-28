import influxdb
import queue
import threading
import psycopg2
from multiprocessing.dummy import Pool
import datetime
import math
import random
import subs.subs
import asyncio
import sys


seen = queue.Queue()
cores = 4
limit = 1000
pool = Pool(cores)


def influxdb_json_body(name, subscribers):
    return {
        'measurement': 'subs',
        'tags': {
            'name': name
        },
        'time': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fields': {
            'subs': subscribers
        }
    }


def influxdb_daemon():
    client = influxdb.InfluxDBClient('localhost', 8086, 'admin', 'admin', 'youtube')

    while True:
        json_single = seen.get(block=True)
        client.write_points([json_single])
        print(json_single['time'], json_single['tags']['name'], 'subs', json_single['fields']['subs'])


def channels():
    conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
    sql = 'SELECT channel_id FROM youtube.channels.channels'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x[0] for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


async def async_get(chan):
    try:
        title, stat = subs.subs.subs(chan)
        bodies = influxdb_json_body(title, stat)
        seen.put(bodies)
    except Exception as e:
        print(e, file=sys.stderr)


def send(chan):
    asyncio.run(async_get(chan))


def priority(chans):
    priority = [math.ceil(math.pow(len(chans) / (1 + x), 2)/len(chans)) for x in range(len(chans))]
    priority_chans = []
    for i in range(len(priority)):
        prior = priority[i]
        for j in range(prior):
            priority_chans.append(chans[i])

    random.shuffle(priority_chans)
    return priority_chans


def main():
    threading.Thread(target=influxdb_daemon, daemon=True).start()

    chans = channels()[:limit]
    priority_chans = priority(chans)

    while True:
        pool.map(send, priority_chans)


main()
