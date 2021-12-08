import time
import csv
import os
import setting

from decimal import Decimal
from MetricsQuery import getMetric
from utils import get_mysql_cursor

node_header = ['id', 'node_name', 'time', 'node_cpu_rate', 'node_load1m',
               'node_load5m', 'node_load15m', 'node_disk_rate',
               'node_ram_rate', 'node_network_rcv_bytes',
               'node_network_transmit_bytes']

container_header = ['id', 'container_name', 'time', 'container_cpu_usage_time', 'container_rss_bytes',
                    'container_load10s', 'container_network_rcv_bytes',
                    'container_network_transmit_bytes', 'container_fs_read_bytes',
                    'container_fs_write_bytes']

nodeQueries = ['node_cpu_rate', 'node_load1m', 'node_load5m',
               'node_load15m', 'node_disk_rate', 'node_ram_rate',
               'node_network_rcv_bytes', 'node_network_transmit_bytes']

containerQueries = ['container_cpu_usage_time', 'container_rss_bytes',
                    'container_load10s', 'container_network_rcv_bytes',
                    'container_network_transmit_bytes', 'container_fs_read_bytes',
                    'container_fs_write_bytes']

node_insert_sql = 'insert into node_metrics values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

container_insert_sql = 'insert into container_metrics values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

# mysql 连接参数
host = setting.host
port = setting.port
user = setting.user
passwd = setting.passwd
db = setting.db


def save_to_csv(monitor, values):
    """
    数据存入 csv 文件,
    拟定针对每个节点创建一个文件,
    针对每个容器再创建一个文件3个节点（或2个） + 42个容器，共45个csv文件

    :param monitor  传入的被监控对象名称
    :param values  被监控的各项指标数据列表
    :param cur  mysql游标
    :return  ./data/*.csv
    """
    path = r"./data/"
    if monitor.startswith('k8s'):
        path = path + "node_metrics.csv"
        if not os.path.isfile(path):
            with open(path, "w+", newline='') as file:
                csv_file = csv.writer(file)
                csv_file.writerow(node_header)
                csv_file.writerow(values)
        else:
            with open(path, "a+", newline='') as file:
                csv_file = csv.writer(file)
                csv_file.writerow(values)
    else:
        path = path + "container_metrics.csv"
        if not os.path.isfile(path):
            with open(path, "w+", newline='') as file:
                csv_file = csv.writer(file)
                csv_file.writerow(container_header)
                csv_file.writerow(values)
        else:
            with open(path, "a+", newline='') as file:
                csv_file = csv.writer(file)
                csv_file.writerow(values)


def save_to_mysql(monitor, values):
    """
    数据存入 mysql  数据库
    :param monitor  传入的被监控对象名称
    :param values  被监控的各项指标数据列表
    :param cur  mysql游标
    """
    if monitor.startswith('k8s'):
        conn, cur = get_mysql_cursor(host, port, user, passwd, db)
        cur.execute(node_insert_sql, values)
        conn.commit()
        conn.close()
    else:
        conn, cur = get_mysql_cursor(host, port, user, passwd, db)
        cur.execute(container_insert_sql, values)
        conn.commit()
        conn.close()


def save_to_storage(monitor, values):
    """
    数据存入 csv  文件以及 mysql 数据库
    :param monitor  传入的被监控对象名称
    :param values  被监控的各项指标数据列表
    """
    save_to_csv(monitor, values)
    # save_to_mysql(monitor, values)


def queryToStorage(duration=1, nodeIp="", container="", nodeDict={}, serviceIp=""):
    """
    查询并存入 csv 以及 mysql
    :param duration  查询的时间跨度
    :param nodeIp  待查询的节点地址
    :param container  待查询的容器名
    :param serviceIp  查询的 api 地址
    :param nodeDict  key=服务器ip, value=服务器节点
    :param cur  mysql游标
    :return  ./data/*.csv
    """

    # 数据列表，字段排列与 header 顺序相同
    result = []
    # 结果第一个值为查询执行的时间点
    timestamp = float(round(time.time() * 1000))

    # 如果是查询 node 相关，保存到 node 的 csv 以及数据库
    if len(nodeIp) != 0:
        # 每行首两项为 id 和 time 时间戳
        if len(nodeDict.keys()) == 0:
            print('nodeDict required!')
            return
        result.append(nodeDict[nodeIp] + '-' + str(timestamp))
        result.append(nodeDict[nodeIp])
        result.append(timestamp / 100000)
        if nodeIp in nodeDict:
            for q in nodeQueries:
                result.append(getMetric(duration=duration, metric=q, serviceIp=serviceIp, nodeIp=nodeIp))
            save_to_storage(nodeDict[nodeIp], result)

    # 如果是查询容器相关，保存到容器的 csv 以及数据库
    if len(container) != 0:
        # 每行首两项为 id 和 time 时间戳
        result.append(container + '-' + str(timestamp))
        result.append(container)
        result.append(timestamp / 100000)
        for q in containerQueries:
            result.append(getMetric(duration=duration, metric=q, serviceIp=serviceIp, container=container))

        save_to_storage(container, result)


def queryToStorage_thread(delay, repeatTimes, duration=1, nodeIp="", container="", nodeDict={}, serviceIp=''):
    """
    循环调用 queryToStorage
    :param delay  循环查询的间隔（秒）
    :param repeatTimes  循环重复次数
    :param duration  查询的时间跨度
    :param nodeIp  待查询的节点地址
    :param container  待查询的容器名
    :param nodeDict  key=服务器ip, value=服务器节点
    :param serviceIp  查询的 api 地址
    :param cur  mysql游标

    :return ./data/*.csv
    """
    for i in range(repeatTimes):
        queryToStorage(duration, nodeIp, container, nodeDict, serviceIp)
        time.sleep(delay)
