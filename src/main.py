import influxdb
import queue
import threading
import psycopg2
from multiprocessing.dummy import Pool
import datetime
import math
import random
import asyncio
import sys
import os
import requests
import json


seen = queue.Queue()
cores = 4
limit = 1000
pool = Pool(cores)


def influxdb_json_body(name, fields):
    return {
        'measurement': 'Channels',
        'tags': name,
        'time': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fields': fields
    }


def influxdb_daemon():
    client = influxdb.InfluxDBClient('localhost', 8086, 'admin', 'admin', 'youtube')

    while True:
        json_single = seen.get(block=True)
        client.write_points(json_single)
        print('Insert at', datetime.datetime.now())


def channels():
    conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
    sql = 'SELECT channel_id FROM youtube.channels.channels'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x[0] for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


async def async_get(chans):
    try:
        json_body = chan_stats(chans)
        stats = get_stats(json_body)
        titles = get_title(json_body)

        bodies = []
        for i in range(len(stats)):
            bodies.append(influxdb_json_body({'name': titles[i]}, stats[i]))

        seen.put(bodies)
    except Exception as e:
        print(e, file=sys.stderr)


def send(chans):
    asyncio.run(async_get(chans))


def priority(chans):
    priority = [math.ceil(math.pow(len(chans) / (1 + x), 2)/len(chans)) for x in range(len(chans))]
    priority_chans = []
    for i in range(len(priority)):
        prior = priority[i]
        for j in range(prior):
            priority_chans.append(chans[i])

    random.shuffle(priority_chans)
    return priority_chans


def chan_stats(chans):
    key = os.environ['API_KEY']
    url = 'https://www.googleapis.com/youtube/v3/channels'
    params = {
        'part': 'snippet,statistics',
        'id': ','.join(chans),
        'key': key
    }

    req = requests.get(url, params=params)
    json_bod = json.loads(req.text)
    return json_bod


def get_stats(json_body):
    stats = json_body['items']

    stats_body = []
    for s in stats:
        stats_tmp = s['statistics']
        stats_body.append({
            'viewCount': int(stats_tmp['viewCount']),
            'subscriberCount': int(stats_tmp['subscriberCount']),
            'videoCount': int(stats_tmp['videoCount'])
        })

    return stats_body


def get_title(json_body):
    items = json_body['items']

    titles = []
    for i in items:
        titles.append(i['snippet']['title'])

    return titles


def chunks(l):
    for i in range(0, len(l), 50):
        yield l[i:i + 50]


def main():
    threading.Thread(target=influxdb_daemon, daemon=True).start()

    chans = channels()[:limit]
    priority_chans = priority(chans)

    while True:
        chunky = list(chunks(priority_chans))
        pool.map(send, chunky)


main()
