import psycopg2
import math
import random


def query_channels(limit):
    conn = psycopg2.connect(user='root', password='', host='127.0.0.1', port='5432', database='youtube')
    sql = f'SELECT channel_id FROM youtube.channels.channels LIMIT {limit}'
    cursor = conn.cursor()
    cursor.execute(sql)
    records = [x[0] for x in cursor.fetchall()]

    cursor.close()
    conn.close()

    return records


def calc_prior(length, x):
    return math.ceil(math.pow(length / (1 + x), 2) / length)


def priority(chans):
    length = len(chans)

    prior = [calc_prior(length, x) for x in range(length)]
    prior_chans = []
    for i in range(length):
        prior_count = prior[i]
        for j in range(prior_count):
            prior_chans.append(chans[i])

    random.shuffle(prior_chans)
    return prior_chans


def total_chans(limit):
    chan = query_channels(limit)
    return priority(chan)
