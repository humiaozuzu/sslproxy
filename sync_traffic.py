import sys
import logging
import time
import imp
import getopt
import collections
import json
import traceback
from pygtail import Pygtail
import sentry_sdk
from sentry_sdk import capture_exception

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    import urllib.request as urllib2
    from urllib.parse import urlencode
else:
    import urllib2
    from urllib import urlencode

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
        logging.info(dict(TrafficSync.statistics))

    @staticmethod
    def sync_traffic():
        dt_transfer = TrafficSync.statistics
        if dt_transfer:
            # apply ratio
            ratio_transfer = {}
            for k in dt_transfer.keys():
                ratio_transfer[k] = config.TRANSFER_RATIO * dt_transfer[k]

            # upload stats
            payload = {
                'token': config.SYNC_TOKEN,
                'uid_data': json.dumps(ratio_transfer),
            }
            resp = urllib2.urlopen(config.SYNC_API_URL + '/v1/sync/traffic', urlencode(payload).encode("utf-8"))
            if resp.code != 200:
                raise RuntimeError(json.load(resp))

    @staticmethod
    def thread_db(conf):
        global config
        config = conf
        import socket
        timeout = 30
        socket.setdefaulttimeout(timeout)
        if config.SENTRY_DSN:
            sentry_sdk.init(dsn=config.SENTRY_DSN)
        while True:
            logging.info('logtail loop')
            try:
                TrafficSync.get_instance().tailf_log()
                TrafficSync.get_instance().sync_traffic()
                TrafficSync.statistics.clear()
            except Exception as e:
                TrafficSync.statistics.clear()
                logging.warn('Clear traffic statics as Exception occurs')
                traceback.print_exc()
                logging.warn('db thread except:%s' % e)
                if config.SENTRY_DSN:
                    capture_exception()
            finally:
                time.sleep(config.TRAFFIC_SYNC_INTERVAL)

def main():
    shortopts = 'hc:'
    longopts = ['help', 'config']
    optlist, args = getopt.getopt(sys.argv[1:], shortopts, longopts)

    config_path = None
    for o, a in optlist:
        if o in ('-h', '--help'):
            print('Usage: sync_traffic.py -c path_to_config.py')
            sys.exit(0)
        elif o in ('-c', '--config'):
            config_path = a

    if not config_path:
        print('config not specified')
        sys.exit(2)

    config = imp.load_source('config', config_path)

    level = config.TRAFFIC_SYNC_LOG_LEVEL
    logging.basicConfig(level=level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    TrafficSync.thread_db(config)

if __name__ == '__main__':
    main()
