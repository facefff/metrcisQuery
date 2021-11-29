import MySQLdb


def invert_dict(d):
    return dict((v, k) for k, v in d.items())


def get_mysql_cursor(host, port, user, passwd, db):
    conn = MySQLdb.connect(
        host=host,
        port=port,
        user=user,
        passwd=passwd,
        db=db,
    )
    # 创建游标，要用游标保持数据，必要时每次 fetch_one 获取单个数据
    cur = conn.cursor(MySQLdb.cursors.DictCursor)

    return conn, cur
