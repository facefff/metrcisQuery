# coding=utf-8
import logging
import MySQLdb
from decimal import Decimal


def init_dict(data_dict, trace_id, container_columns):
    # 除了 trace_id 之外，初始化每个 key 对应一个列表
    data_dict['trace_id'] = list()
    data_dict['trace_id'].append(trace_id)
    data_dict['RT'] = list()
    data_dict['start_time'] = list()
    data_dict['end_time'] = list()
    data_dict['is_error'] = list()
    data_dict['spans'] = list()
    for column in container_columns:
        if column['Field'] in ['id', 'container_name', 'time']:
            continue
        data_dict[column['Field']] = list()
    return data_dict


def append_data(data_dict, span_dict, is_root, cursor=None, sql='', invoke_chain=[]):
    """
    非root节点需要传入cursor以及sql用于查询获取span相关的container指标
    """
    logger = logging.getLogger('Trying to append trace data')
    if is_root:
        data_dict['start_time'].append(span_dict['start_time'])
        data_dict['end_time'].append(span_dict['end_time'])
        data_dict['is_error'].append(span_dict['is_error'])
    else:
        data_dict['spans'].append((invoke_chain[-2], invoke_chain[-1]))
        data_dict['RT'].append(span_dict['end_time'] - span_dict['start_time'])
        cursor.execute(sql, args=(span_dict['service_name'], span_dict['start_time'] - Decimal(0.00003),
                                  span_dict['end_time'] + Decimal(0.00003)))
        container_metrics = cursor.fetchall()
        if len(container_metrics) != 0:
            for k in container_metrics[0].keys():
                if k in ['id', 'container_name', 'time']:
                    continue
                data_dict[k].append(container_metrics[k])
        else:
            logger.error("container {}, time_range: {} -- {} ,数据未查到！".format(span_dict['service_name'],
                                                                             span_dict['start_time'] - Decimal(0.00003),
                                                                             span_dict['end_time'] + Decimal(0.00003)))


def main():
    """
    前置：
    container_metrics保存到本地 mysql
    node_metrics保存到本地 mysql
    trace保存到本地 mysql
    这意味着从prometheus获取数据的程序应该在 trace 生成时保持运行
    """

    conn = MySQLdb.connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='root123456',
        db='data',
    )
    # 创建多个游标，用以维持多个数据，trace 相关的操作都是需要遍历查询的
    # 所以要用游标保持数据，每次 fetch_one 获取数据
    cur1 = conn.cursor(MySQLdb.cursors.DictCursor)
    cur2 = conn.cursor(MySQLdb.cursors.DictCursor)

    # 获取所有的 trace_id
    sql1 = "select distinct trace_id from traces"
    # 遍历获取每个 trace 的调用链条
    sql2 = "select * from traces where trace_id=%s order by start_time"
    # 遍历计算每次调用的时间段内，对应微服务的相关指标数据
    sql3 = "select * from container_metrics where container_name=%s and time between %s and %s"
    # 获取 container_metrics 的字段名
    sql4 = "SHOW COLUMNS FROM container_metrics"

    # 获取所有的 trace_id
    cur1.execute(sql1)
    cur2.execute(sql4)
    trace_ids = cur1.fetchall()
    container_columns = cur2.fetchall()

    logger = logging.getLogger('Trying to build invoke chain.')

    for t_id in trace_ids:
        # 根据 trace_id 获取 trace 数据
        cur1.execute(sql2, [(t_id['trace_id'])])
        spans = cur1.fetchall()
        parent_id = '-1'

        invoke_chain = list()
        trace_data_dict = dict()
        trace_data_dict = init_dict(trace_data_dict, t_id['trace_id'], container_columns)

        print('-' * 50)
        print('Len of this trace is : %s' % len(spans))
        # 构建调用链
        # 目前构建的是一条按时间排序的链条，没有树状结构
        for span in spans:
            # 在 Entry 处串联 span
            # 除了第一次 Entry 外，以后每次进入时计算
            # 响应时间 RT = start_time - end_time
            if span['type'] == 'Entry':
                # 入口服务
                if parent_id == '-1':
                    invoke_chain.append(span['service_name'])
                    append_data(trace_data_dict, span, True)
                # 后续服务接入，从第二个服务开始构建调用链
                elif parent_id == span['unique_parent_id']:
                    invoke_chain.append(span['service_name'])
                    append_data(trace_data_dict, span, False, cur1, sql3, invoke_chain)
                # 当前 Entry 和上一处 Exit 不匹配
                else:
                    logger.info("当前span与上一span不匹配！")
            else:
                parent_id = span['unique_id']
        # trace 数据只有一个服务时，认为是自调用，span 定为自调用 span
        if len(invoke_chain) == 1:
            trace_data_dict['spans'].append((invoke_chain[-1], invoke_chain[-1]))
        print('The invoke chain structured shows below: the length is : %s' % len(invoke_chain))
        # print(invoke_chain)
        print(trace_data_dict)

    cur1.close()
    cur2.close()
    conn.commit()
    conn.close()


if __name__ == '__main__':
    main()
