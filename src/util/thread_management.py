import asyncio
import sys
import threading
import traceback
from multiprocessing.dummy import Pool

import util.influx
import util.http_tools

cores = 1
pool = Pool(cores)


async def async_get(chans):
    try:
        json_body = util.http_tools.json_bod(chans)
        stats = util.http_tools.stats(json_body)
        titles = util.http_tools.title(json_body)

        bodies = []
        for i in range(len(stats)):
            bodies.append(util.influx.json({'name': titles[i]}, stats[i]))

        util.influx.seen.put(bodies)
    except Exception as e:
        print(e, file=sys.stderr)
        traceback.print_exc()


def send(chans):
    asyncio.run(async_get(chans))


def chunks(l):
    for i in range(0, len(l), 50):
        yield l[i:i + 50]


def loop(chans, num_cores):
    global cores
    cores = num_cores

    while True:
        chunky = list(chunks(chans))
        pool.map(send, chunky)


def start_influx_insert_daemon():
    threading.Thread(target=util.influx.start, daemon=True).start()
