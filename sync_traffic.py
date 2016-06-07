import sqlite3
import cymysql
import sys
import logging
import time
import imp
import getopt
import collections
from pygtail import Pygtail

config = None


class TrafficSync(object):

    statistics = collections.defaultdict(int)
    instance = None

    @staticmethod
    def get_instance():
        if TrafficSync.instance is None:
            TrafficSync.instance = TrafficSync()
        return TrafficSync.instance

    @staticmethod
    def tailf_log():
        for line in Pygtail(config.SQUID_LOG, config.SQUID_LOG_OFFSET):
            try:
                _, _, _, code_status, num_bytes, _, _, rfc931, _, _ = line.split()[:10]
            except ValueError:
                logging.warn('error parsing line: %s' % line)
                continue
            # unauthorized user
            if rfc931 == '-': continue
            # wrong username and/or password
            if code_status.split('/')[1] == '407': continue

            TrafficSync.statistics[rfc931] += int(num_bytes)

    @staticmethod
    def sync_traffic():
        dt_transfer = TrafficSync.statistics
        query_head = 'UPDATE user'
        query_sub_when = ''
        query_sub_when2 = ''
        query_sub_in = None
        last_time = time.time()
        for id in dt_transfer.keys():
            query_sub_when += ' WHEN "%s" THEN u+%s' % (id, 0) # all in d
            query_sub_when2 += ' WHEN "%s" THEN d+%s' % (id, int(dt_transfer[id] * config.TRANSFER_RATIO))
            if query_sub_in is not None:
                query_sub_in += ',"%s"' % id
            else:
                query_sub_in = '"%s"' % id
        if query_sub_when == '':
            return
        query_sql = query_head + ' SET u = CASE username' + query_sub_when + \
                    ' END, d = CASE username' + query_sub_when2 + \
                    ' END, t = ' + str(int(last_time)) + \
                    ' WHERE username IN (%s)' % query_sub_in
        # print query_sql
        conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                               passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        cur = conn.cursor()
        cur.execute(query_sql)
        cur.close()
        conn.commit()
        conn.close()

    @staticmethod
    def thread_db(conf):
        global config
        config = conf
        import socket
        timeout = 30
        socket.setdefaulttimeout(timeout)
        while True:
            logging.info('logtail loop')
            try:
                TrafficSync.get_instance().tailf_log()
                TrafficSync.get_instance().sync_traffic()
                TrafficSync.statistics.clear()
            except Exception as e:
                import traceback
                traceback.print_exc()
                logging.warn('db thread except:%s' % e)
            finally:
                time.sleep(config.TRAFFIC_SYNC_INTERVAL)

def main():
    shortopts = 'hc:'
    longopts = ['help', 'config']
    optlist, args = getopt.getopt(sys.argv[1:], shortopts, longopts)

    config_path = None
    for o, a in optlist:
        if o in ('-h', '--help'):
            print 'Usage: sync_traffic.py -c path_to_config.py'
            sys.exit(0)
        elif o in ('-c', '--config'):
            config_path = a

    if not config_path:
        print 'config not specified'
        sys.exit(2)

    config = imp.load_source('config', config_path)

    level = config.TRAFFIC_SYNC_LOG_LEVEL
    logging.basicConfig(level=level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    TrafficSync.thread_db(config)

if __name__ == '__main__':
    main()
