import asyncio
import sys
import threading
import traceback
from multiprocessing.dummy import Pool

import http_tools.util
import influx.util

cores = 4
pool = Pool(cores)


async def async_get(chans):
    try:
        json_body = http_tools.util.json_bod(chans)
        stats = http_tools.util.stats(json_body)
        titles = http_tools.util.title(json_body)

        bodies = []
        for i in range(len(stats)):
            bodies.append(influx.util.json({'name': titles[i]}, stats[i]))

        influx.util.seen.put(bodies)
    except Exception as e:
        print(e, file=sys.stderr)
        traceback.print_exc()


def send(chans):
    asyncio.run(async_get(chans))


def chunks(l):
    for i in range(0, len(l), 50):
        yield l[i:i + 50]


def loop(chans):
    while True:
        chunky = list(chunks(chans))
        pool.map(send, chunky)


def start_influx_insert_daemon():
    threading.Thread(target=influx.util.start, daemon=True).start()
