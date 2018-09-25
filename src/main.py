import requests
import json
import psycopg2
import influxdb
import queue
import threading
import psycopg2
import random
from multiprocessing.dummy import Pool
import datetime
import os


seen = queue.Queue()
cores = 8
pool = Pool(cores)


def influxdb_json_body(measure_name, tags, fields):
    return [{
        'measurement': measure_name,
        'tags': tags,
        'time': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fields': fields
    }]


def influxdb_daemon():
    client = influxdb.InfluxDBClient('localhost', 8086, 'admin', 'admin', 'youtube')

    while True:
        json_bod = seen.get(block=True)
        client.write_points(json_bod)


def channels():
    conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
    sql = 'SELECT channel_id FROM youtube.channels.channels'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x[0] for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


def chan_stats(chan):
    key = os.environ['API_KEY']
    url = 'https://www.googleapis.com/youtube/v3/channels'
    params = {
        'part': 'snippet,statistics',
        'id': chan,
        'key': key
    }

    req = requests.get(url, params=params)
    json_bod = json.loads(req.text)
    return json_bod


def get_stats(json_body):
    stats = json_body['items'][0]['statistics']
    return {
        'viewCount': int(stats['viewCount']),
        'subscriberCount': int(stats['subscriberCount']),
        'videoCount': int(stats['videoCount'])
    }


def get_title(json_body):
    return json_body['items'][0]['snippet']['title']


def send(chan):
    json_body = chan_stats(chan)
    stats = get_stats(json_body)
    title = get_title(json_body)
    influxdb_payload = influxdb_json_body(title, {
        'id': chan,
    }, stats)
    print(influxdb_payload)

    seen.put(influxdb_payload)

def main():
    threading.Thread(target=influxdb_daemon, daemon=True).start()

    chans = channels()
    top_10000 = chans[:1000]

    while True:
        for chan in top_10000:
            send(chan)



main()
