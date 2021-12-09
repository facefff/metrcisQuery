import csv
import os
import setting
from elasticsearch import Elasticsearch
from TargetQuery import getContainers
from datetime import datetime

# elasticsearch集群服务器的地址
ES = setting.ES_host
# 创建elasticsearch客户端
es = Elasticsearch(ES)
# 查询的索引
index = 'logstash-2021.12.09'
# 查询的字段
fields = ["@timestamp", "kubernetes.namespace_name", "kubernetes.container_name", "log"]
# 查询的命名空间
namespace = "train-.*"
# 写文件的header
header = ["timestamp", "namespace", "container_name", "log_line"]
# 需要多少条
data_num = 600
# 服务列表
serviceList = {}
# 服务列表文件
path = setting.target_path + 'images.txt'

getContainers(setting.serviceIp)
if os.path.isfile(path):
    with open(path, "r+") as f:
        serviceList = f.readline().split(',')

print(serviceList)
# 对每个服务分别进行查询
for svc in serviceList:
    for i in range(int(data_num / 3000) + 1):
        if (data_num - 3000 * i) > 3000:
            num = 3000
        else:
            num = max(data_num - 3000 * i, 0)
        print("target num")
        print(num)
        filePath = r'./data/' + svc + '_logs.csv'
        query = {
            '_source': fields,
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "kubernetes.namespace_name": namespace
                            }
                        },
                        {
                            "term": {
                                "kubernetes.container_name.keyword": {
                                    "value": svc
                                }
                            }
                        }
                    ]
                }
            },
            "sort": [
                {
                    "@timestamp": {
                        "order": "asc"
                    }
                }
            ],
            "from": i + 1,
            "size": num
        }
        ret = es.search(index=index, body=query)
        print("hits num:")
        print(ret['hits']['total']['value'])
        if ret['hits']['total']['value'] == 0:
            continue
        else:
            for doc in ret['hits']['hits']:
                data = list()
                for f in fields:
                    s = f.split('.')
                    if len(s) == 1:
                        data.append(doc['_source'][s[0]].strip())
                    else:
                        data.append(doc['_source'][s[0]][s[1]].strip())
                # 考虑加入对每个服务数据存文件的操作
                if os.path.isfile(filePath):
                    with open(filePath, "a+", newline='') as file:
                        csv_file = csv.writer(file)
                        csv_file.writerow(data)
                        file.close()
                else:
                    with open(filePath, "w+", newline='') as file:
                        csv_file = csv.writer(file)
                        csv_file.writerow(header)
                        csv_file.writerow(data)
                        file.close()
