import sqlite3
import cymysql
import sys
import logging
import time
import imp
import getopt

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
        r_conn = cymysql.connect(host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_USER,
                                 passwd=config.MYSQL_PASS, db=config.MYSQL_DB, charset='utf8')
        try:
            r_cur = r_conn.cursor()
            r_cur.execute("SELECT username, u, d, transfer_enable, passwd, switch, ssl_enabled FROM user;")
            # for r in cur.fetchall():
            #     rows.append(list(r))
            r_users = r_cur.fetchall()
            r_cur.close()
        finally:
            r_conn.close()
        # print r_users

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
        for user in r_users:
            if user[0] in l_users_dict.keys():
                # user in local, check stat if need to remove
                if user[5] == 0 or user[6] == 0:
                    logging.info('stop user %s as server disabled' % user[0])
                    del_ids.append(user[0])
                elif user[1] + user[2] >= user[3]:
                    logging.info('stop user %s as bandwidth exceeded' % user[0])
                    del_ids.append(user[0])
                elif user[4] != l_users_dict[user[0]][1]:
                    logging.info('update user %s as password changed' % user[0])
                    del_ids.append(user[0])
                    add_list.append((user[0], user[4], 1))
            else:
                # new user, check if need to add
                if user[5] == 1 and user[6] == 1 and user[1] + user[2] < user[3]:
                    logging.info('add user %s' % user[0])
                    add_list.append((user[0], user[4], 1))

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
            print 'Usage: sync_user.py -c path_to_config.py'
            sys.exit(0)
        elif o in ('-c', '--config'):
            config_path = a

    if not config_path:
        print 'config not specified'
        sys.exit(2)

    config = imp.load_source('config', config_path)

    level = config.USER_SYNC_LOG_LEVEL
    logging.basicConfig(level=level,
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    UserSync.thread_db(config)

if __name__ == '__main__':
    main()
