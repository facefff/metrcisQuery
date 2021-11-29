import logging
import requests
import re


def getTargetsStatus(serviceIp=''):
    """
    获取主机IP，去掉宕机的节点
    :param serviceIp: 查询的 api 地址
    :return: 存活主机列表，宕机主机列表
    """

    url = 'http://' + serviceIp + '/api/v1/targets'
    logger = logging.getLogger('getting targets.')
    logger.info("获取节点IP地址：%s" % url)
    response = requests.request('GET', url)
    aliveNum, totalNum = 0, 0
    # 存活主机列表
    uplist = []
    # 宕机主机列表
    downList = []
    if response.status_code == 200:
        targets = response.json()['data']['activeTargets']
        for target in targets:
            totalNum += 1
            if target['health'] == 'up':
                aliveNum += 1
                uplist.append(target['discoveredLabels']['__address__'].split(':')[0])
            else:
                downList.append(target['labels']['instance'].split(':')[0])

    # 去掉localhost
    try:
        if uplist.index('localhost') is not None:
            uplist.remove('localhost')
    except ValueError:
        logger.info("localhost项不存在！")
    else:
        logger.info("节点信息读取完成！")

    return uplist, downList


def getNodeExporterIp(serviceIp=''):
    """
    获取 k8s 内 node-exporter 的IP
    :param serviceIp: 查询的 api 地址
    :return: 节点名 : 节点ip的字典
    """

    url = 'http://' + serviceIp + '/api/v1/targets'
    logger = logging.getLogger('getting nodes.')
    logger.info("获取node-exporter IP地址：%s" % url)
    response = requests.request('GET', url)

    exporter_dict = {}
    if response.status_code == 200:
        targets = response.json()['data']['activeTargets']
        for target in targets:
            if target['health'] == 'up':
                if '__meta_kubernetes_endpoint_address_target_name' in target['discoveredLabels']:
                    end_point_name = target['discoveredLabels']['__meta_kubernetes_endpoint_address_target_name']
                else:
                    continue
                if re.match('^node-exporter-[\s\S]*', end_point_name) != None:
                    exporter_dict[target['discoveredLabels']['__meta_kubernetes_endpoint_node_name']] = \
                        target['discoveredLabels']['__address__'].split(':')[0]

    with open('./targets/nodes.txt', 'w+') as f:
        f.write(exporter_dict['k8s-node1'] + ',' + exporter_dict['k8s-node2'])
        f.close()

    return exporter_dict


def getContainers(serviceIp=''):
    image_path = r'./targets/images.txt'

    url = "http://" + serviceIp + "/api/v1/query?query=kube_pod_container_info{container=~\"ts-.*\"}"
    response = requests.request('GET', url)

    with open(image_path, mode='w') as f:
        for c in response.json()['data']['result']:
            f.write(c['metric']['container'] + ',')
        f.close()
