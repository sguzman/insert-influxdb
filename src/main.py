import util.thread_management
import util.postgresql

limit = 1000
cores = 1


def main():
    util.thread_management.start_influx_insert_daemon()
    chans = util.postgresql.total_chans(limit)

    util.thread_management.loop(chans, cores)


main()
