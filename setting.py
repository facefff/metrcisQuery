# -*- encoding: utf-8 -*-

# log 日期格式
logformat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# query 参数
serviceIp = "10.236.101.12:30003"  # prometheus地址
queryInterval = 5  # 查询间隔，默认5s
repeatTimes = 10000  # 重复查询次数，默认时5s * 10000
queryDuration = '1m'  # 在prometheus查询的时间跨度，不用改，默认为1min

# mysql 连接参数
host = 'localhost'
port = 3306
user = 'root'
passwd = 'root123456'
db = 'data'
