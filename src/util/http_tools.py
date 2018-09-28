import os
import json
import requests


def json_bod(chans):
    key = os.environ['API_KEY']
    url = 'https://www.googleapis.com/youtube/v3/channels'
    params = {
        'part': 'snippet,statistics',
        'id': ','.join(chans),
        'key': key
    }

    req = requests.get(url, params=params)
    json_body = json.loads(req.text)
    return json_body


def stats(json_body):
    stats_result = json_body['items']

    stats_body = []
    for s in stats_result:
        stats_tmp = s['statistics']
        stats_body.append({
            'viewCount': int(stats_tmp['viewCount']),
            'subscriberCount': int(stats_tmp['subscriberCount']),
            'videoCount': int(stats_tmp['videoCount'])
        })

    return stats_body


def title(json_body):
    items = json_body['items']

    titles = []
    for i in items:
        titles.append(i['snippet']['title'])

    return titles
