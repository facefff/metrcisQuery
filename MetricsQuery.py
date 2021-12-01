import requests
import logging
import datetime


def getMetric(duration, metric, serviceIp='', nodeIp="", container=""):
    """
    :param duration 查询的时间跨度
    :param metric 要查询的指标名
    :param serviceIp 查询的 api 地址
    :param nodeIp 待查询的节点地址
    :param container 待查询的容器名
    :return 查询结果值，id为容器名 + 时间戳，time为查询操作执行时的时间戳
    """

    # 请求前半截
    head = 'http://' + serviceIp + '/api/v1/query?query='

    # 节点cpu使用率(1-(cpu空闲时间 / 总的cpu时间))
    if metric == 'node_cpu_rate':
        expr = '(1- sum(increase(node_cpu_seconds_total{instance=~"' + nodeIp + '.*",mode="idle"}[' + str(
            duration) + 'm])) by (instance) ' \
                        '/ sum(increase(node_cpu_seconds_total{instance=~"' + nodeIp + '.*"}[' + str(
            duration) + 'm])) by (instance)) * 100'

    # 节点load_average，1分钟
    elif metric == 'node_load1m':
        expr = 'node_load1{instance=~"' + nodeIp + '.*"}'

    # 节点load_average，5分钟
    elif metric == 'node_load5m':
        expr = 'node_load5{instance=~"' + nodeIp + '.*"}'

    # 节点load_average，15分钟
    elif metric == 'node_load15m':
        expr = 'node_load15{instance=~"' + nodeIp + '.*"}'

    # 节点磁盘使用率，指定了磁盘为mapper逻辑卷
    elif metric == 'node_disk_rate':
        expr = 'avg by (instance)(1-(node_filesystem_free_bytes{instance=~"' + nodeIp + '.*",fstype=~"ext4|xfs",device=~"/dev/mapper.*"} ' \
                                                                                        '/ node_filesystem_size_bytes{instance=~"' + nodeIp + '.*",fstype=~"ext4|xfs",device=~"/dev/mapper.*"})) * 100'

    # 节点内存使用率
    elif metric == 'node_ram_rate':
        expr = '(node_memory_MemTotal_bytes{instance=~"' + nodeIp + '.*"} - node_memory_MemAvailable_bytes{instance=~"' + nodeIp + '.*"}) ' \
                                                                                                                                   '/ node_memory_MemTotal_bytes{instance=~"' + nodeIp + '.*"} * 100'

    # 节点前一分钟接收的网络数据量
    elif metric == 'node_network_rcv_bytes':
        expr = 'increase(node_network_receive_bytes_total{instance=~"' + nodeIp + '.*", device!="lo"}[1m])'

    # 节点前一分钟发出的网络数据量
    elif metric == 'node_network_transmit_bytes':
        expr = 'increase(node_network_transmit_bytes_total{instance=~"' + nodeIp + '.*", device!="lo"}[1m])'

    # container容器前一分钟占用的cpu时间(8个核加起来总的)
    elif metric == 'container_cpu_usage_time':
        expr = 'sum by (container_label_io_kubernetes_container_name)' \
               '(increase(container_cpu_usage_seconds_total{container_label_io_kubernetes_container_name=~"' + container + '.*"}[1m]))'

    # container容器当前使用的物理内存 bytes
    elif metric == 'container_rss_bytes':
        expr = 'sum by (container_label_io_kubernetes_container_name)' \
               '(container_memory_rss{container_label_io_kubernetes_container_name=~"' + container + '.*"})'

    # container容器 load_average 10s
    elif metric == 'container_load10s':
        expr = 'sum by (container_label_io_kubernetes_container_name)' \
               '(container_cpu_load_average_10s{container_label_io_kubernetes_container_name=~"' + container + '.*"})'

    # container容器前一分钟接收的网络数据量
    elif metric == 'container_network_rcv_bytes':
        expr = 'increase(container_network_receive_bytes_total{container_label_io_kubernetes_pod_name=~"' + container + '.*"}[1m])'

    # container容器前一分钟发出的网络数据量
    elif metric == 'container_network_transmit_bytes':
        expr = 'increase(container_network_transmit_bytes_total{container_label_io_kubernetes_pod_name=~"' + container + '.*"}[1m])'

    # container容器前一分钟的文件写 bytes
    elif metric == 'container_fs_write_bytes':
        expr = 'sum by (container_label_io_kubernetes_container_name)' \
               '(increase(container_fs_writes_bytes_total{container_label_io_kubernetes_container_name=~"' + container + '.*"}[1m]))'

    # container容器前一分钟的文件读 bytes
    elif metric == 'container_fs_read_bytes':
        expr = 'sum by (container_label_io_kubernetes_container_name)' \
               '(increase(container_fs_reads_bytes_total{container_label_io_kubernetes_container_name=~"' + container + '.*"}[1m]))'

    # 拼装请求
    url = head + expr

    logger = logging.getLogger('prometheusQuery')
    logger.info("执行查询：%s" % url)

    value = 0

    response = requests.request('GET', url)
    if response.status_code == 200:
        length = len(response.json()['data']['result'])
        if length != 0:
            response = response.json()['data']['result']
            for res in response:
                value += float(res['value'][1])
                # Unix时间戳转成字符串，打包成 [时间，值] 列表
                # t = datetime.datetime.fromtimestamp(res['value'][0]).strftime('%Y-%m-%d %H:%M:%S')
                # time_data = [t, res['value'][1]]
                # value.append(time_data)
            value /= length
        else:
            logger.error(str(nodeIp) + ' : ' + str(container) + ' : ' + metric + "获取结果为空")
    else:
        logger.error(nodeIp + container + metric + "请求失败")

    return value
