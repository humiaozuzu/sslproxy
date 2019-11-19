import sqlite3
import sys
import logging
import time
import imp
import getopt
import json
import gzip
from raven import Client

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    import urllib.request as urllib2
else:
    import urllib2

config = None


class UserSync(object):

    instance = None

    @staticmethod
    def get_instance():
        if UserSync.instance is None:
            UserSync.instance = UserSync()
        return UserSync.instance

    @staticmethod
    def sync_user():
        # get remote users
        params = "token=%s" % config.SYNC_TOKEN
        req = urllib2.Request(config.SYNC_API_URL + '/v1/sync/users', params.encode("utf-8"))
        req.add_header('Accept-encoding', 'gzip')
        resp = urllib2.urlopen(req)
        if resp.info().get('Content-Encoding') == 'gzip':
            data_r = gzip.decompress(resp.read())
            data = json.loads(data_r)
        else:
            data = json.load(resp)
        traffic_ok_users = data['traffic_ok']
        traffic_exceed_users = data['traffic_exceed']
        r_usernames = [user[0] for user in traffic_ok_users] + [user[0] for user in traffic_exceed_users]

        # get local users
        l_conn = sqlite3.connect(config.LOCAL_DB)
        l_cur = l_conn.cursor()
        l_cur.execute('create table if not exists passwd(user varchar(20) not null unique,  password varchar(20) not null, enabled BOOLEAN not null);')
        l_cur.execute('SELECT * FROM passwd;')
        l_users = l_cur.fetchall()
        l_users_dict = {d[0]:d for d in l_users}

        # diff
        add_list = []
        del_ids = []
        # for traffic ok active users, add or change password
        for user in traffic_ok_users:
            if user[0] in l_users_dict.keys():
                if user[2] != l_users_dict[user[0]][1]:
                    logging.info('update user %s as password changed' % user[0])
                    del_ids.append(user[0])
                    add_list.append((user[0], user[2], 1))
            else:
                logging.info('add user %s' % user[0])
                add_list.append((user[0], user[2], 1))
        # for traffic not ok users, disable
        for user in traffic_exceed_users:
            if user[0] in l_users_dict.keys():
                logging.info('stop user %s as bandwidth exceeded' % user[0])
                del_ids.append(user[0])
        # for not in users, remove
        for username in l_users_dict.keys():
            if username not in r_usernames:
                logging.info('stop user %s as service disabled or expired' % username)
                del_ids.append(username)

        # sync to local
        if del_ids:
            l_cur.execute('DELETE FROM passwd WHERE user IN (%s);' % ','.join('?' * len(del_ids)), del_ids)
            l_conn.commit()
        if add_list:
            l_cur.executemany('INSERT INTO passwd VALUES (?,?,?);', add_list)
            l_conn.commit()
        l_cur.close()
        l_conn.close()

    @staticmethod
    def thread_db(conf):
        global config
        config = conf
        import socket
        timeout = 30
        socket.setdefaulttimeout(timeout)
        while True:
            logging.info('db loop')
            try:
                UserSync.get_instance().sync_user()
            except Exception as e:
                import traceback
                traceback.print_exc()
                logging.warn('db thread except:%s' % e)
            finally:
                time.sleep(config.USER_SYNC_INTERVAL)

def main():
    shortopts = 'hc:'
    longopts = ['help', 'config']
    optlist, args = getopt.getopt(sys.argv[1:], shortopts, longopts)

    config_path = None
    for o, a in optlist:
        if o in ('-h', '--help'):
            print('Usage: sync_user.py -c path_to_config.py')
            sys.exit(0)
        elif o in ('-c', '--config'):
            config_path = a

    if not config_path:
        print('config not specified')
        sys.exit(2)

    config = imp.load_source('config', config_path)

    level = config.USER_SYNC_LOG_LEVEL
    logging.basicConfig(level=level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    UserSync.thread_db(config)

if __name__ == '__main__':
    main()
