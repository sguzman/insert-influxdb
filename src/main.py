import requests
import json
import psycopg2
import influxdb
import queue
import threading
import psycopg2
from multiprocessing.dummy import Pool
import datetime
import os


seen = queue.Queue()
cores = 4
pool = Pool(cores)
top_limit = 10000


def influxdb_json_body(measure_name, tags, fields):
    return {
        'measurement': measure_name,
        'tags': tags,
        'time': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fields': fields
    }


def influxdb_daemon():
    client = influxdb.InfluxDBClient('localhost', 8086, 'admin', 'admin', 'youtube')

    while True:
        json_bod = seen.get(block=True)
        client.write_points(json_bod)
        print(datetime.datetime.now())


def channels():
    conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
    sql = 'SELECT channel_id FROM youtube.channels.channels'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x[0] for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


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


def send(chans):
    json_body = chan_stats(chans)
    stats = get_stats(json_body)
    titles = get_title(json_body)

    bodies = []
    for i in range(len(stats)):
        bodies.append(influxdb_json_body(titles[i], {
            'id': chans[i],
        }, stats[i]))

    seen.put(bodies)


def chunks(l):
    for i in range(0, len(l), 50):
        yield l[i:i + 50]


def main():
    threading.Thread(target=influxdb_daemon, daemon=True).start()

    chans = channels()
    top_chans = chans[:top_limit]

    while True:
        chunky = list(chunks(top_chans))
        pool.map(send, chunky)


main()
