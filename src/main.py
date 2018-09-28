import thread_managment.util
import postgresql.util

limit = 1000


def main():
    thread_managment.util.start_influx_insert_daemon()
    chans = postgresql.util.total_chans(limit)

    thread_managment.util.loop(chans)


main()
