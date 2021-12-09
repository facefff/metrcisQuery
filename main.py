import logging
import time
import threading
import os
import setting

from utils import invert_dict
from TargetQuery import getNodeExporterIp, getContainers
from QueryToStorage import queryToStorage_thread


serviceIp = setting.serviceIp  # prometheus地址
queryInterval = setting.queryInterval  # 查询间隔，默认5s
repeatTimes = setting.repeatTimes  # 重复查询次数，默认时5s * 10000
queryDuration = setting.queryDuration  # 在prometheus查询的时间跨度，不用改，默认为1min


def main():
    # 创建数据/待查询目标的目录
    os.makedirs("data", exist_ok=True)
    os.makedirs("targets", exist_ok=True)

    # 配置日志打印格式
    logging.basicConfig(level=logging.INFO, format=setting.logformat)

    name_ip_dict = getNodeExporterIp(serviceIp)
    ip_name_dict = invert_dict(name_ip_dict)
    getContainers(serviceIp)

    # 可以通过访问 http://10.236.101.12:5000/v2/_catalog
    # 获取到镜像列表，镜像和容器同名，进而获取到容器列表
    # 从文本文件读取容器名（实际上为镜像名）
    with open(setting.target_path + "images.txt", "r") as img:
        containers = img.read()
        containers = containers.split(',')
        img.close()

    # 从文本文件读取节点列表
    with open(setting.target_path + "nodes.txt", "r") as f:
        nodes = f.read()
        nodes = nodes.split(',')
        f.close()

    logger = logging.getLogger('threadCreating')
    threads = []

    for no in nodes:
        try:
            threads.append(threading.Thread(target=queryToStorage_thread,
                                            args=(queryInterval, repeatTimes, queryDuration, no, "",
                                                  ip_name_dict, serviceIp)))
        except:
            logger.error("节点线程新建失败")

    for container in containers:
        try:
            threads.append(
                threading.Thread(target=queryToStorage_thread,
                                 args=(queryInterval, repeatTimes, queryDuration, "", container, {}, serviceIp)))
        except:
            logger.error("容器线程新建失败")

    for t in threads:
        t.setDaemon(True)
        t.start()

    # 主线程等待
    time.sleep(queryInterval * repeatTimes)


if __name__ == '__main__':
    main()
