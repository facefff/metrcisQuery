import csv
import os
import setting
from tqdm import tqdm
from elasticsearch import Elasticsearch
from TargetQuery import getContainers
from datetime import datetime

# elasticsearch集群服务器的地址
ES = setting.ES_host
# 创建elasticsearch客户端
es = Elasticsearch(ES)
# 查询的索引
index = 'logstash-2021.12.10'
# 查询的字段
fields = ["@timestamp", "kubernetes.namespace_name", "kubernetes.container_name", "log"]
# 查询的命名空间
namespace = "train-.*"
# 写文件的header
header = ["timestamp", "namespace", "container_name", "log_line"]
# 需要多少条
data_num = 20000
# 每次查多少条
docs_per_search = 3000
# 服务列表
serviceList = {}
# 服务列表文件
path = setting.target_path + 'images.txt'

getContainers(setting.serviceIp)
loglist = [f for f in os.listdir(setting.data_path) if 'log' in f]
# 删除旧日志
for f in loglist:
    os.remove(setting.data_path + f)

if os.path.isfile(path):
    with open(path, "r+") as f:
        serviceList = f.readline().split(',')

# 对每个服务分别进行查询
for svc in tqdm(serviceList):
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
        "from": 0,
        "size": docs_per_search
    }
    for i in range(int(data_num / docs_per_search) + 1):
        if (data_num - docs_per_search * i) > docs_per_search:
            num = docs_per_search
        else:
            num = max(data_num - docs_per_search * i, 0)
        query["size"] = num
        ret = es.search(index=index, body=query)
        if len(ret['hits']['hits']) == 0:
            break
        else:
            query['search_after'] = ret['hits']['hits'][-1]['sort']
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
