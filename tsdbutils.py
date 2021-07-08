"""
Copyright:tncet.com
Author:crayon
Date:2021-06-11
与采集箱通信程序
"""


import datetime
from influxdb import InfluxDBClient


INFLUX_DB = "bwm"
conn_db = InfluxDBClient('localhost', '8086', '', '', INFLUX_DB)


URL_HEADER = "http://localhost:3242"

def input_data(dataList):
    '''
    '''
    try:
        if len(dataList) == 0:
            return
        DATA_BUFFER = []
        dtime = int(datetime.datetime.now().timestamp()*1000)
        for data in dataList:
            eachData = {
                'measurement':'m',
                'time': dtime,
                'fields': {
                    'value': data['xData'],
                },
                'tags':{
                    'id': data['channelId'],
                    'location':'1',
                    'source':'monix'
                }
            }
            DATA_BUFFER.append(eachData)
            eachData = {
                'measurement':'m',
                'time': dtime,
                'fields': {
                    'value': data['yData'],
                },
                'tags':{
                    'id': data['channelId'],
                    'location':'2',
                    'source':'monix'
                }
            }
            DATA_BUFFER.append(eachData)
            if len(DATA_BUFFER) > 180:
                print('DATA_BUFFER',DATA_BUFFER)
                insert_many(DATA_BUFFER)
                DATA_BUFFER = []
        insert_many(DATA_BUFFER)
    except Exception as e:
        print('写数据线程异常',e)

def insert_many(data):
    """
    向tsdb写入数据
    """
    response = conn_db.write_points(data, time_precision='ms')
    if (not response):
        raise Exception('数据写入失败')


def query_single_data(start,end,channel_code):
    if channel_code is None:
        return {}
    if start:
        tsdb_start = start
    else:
        tsdb_start = 1483200000000 #2017-01-01 00:00:00
    q="select * from m where time>{}ms and time<{}ms and id='{}';".format(tsdb_start, end, channel_code)
    response = conn_db.query(q, epoch='ms')
    channel_data = list(response.get_points())
    dataRs = transformChannelDataFormat(channel_data)
    # print('dataRs',dataRs)
    return dataRs


def transformChannelDataFormat(channel_data):
    dataRs = {}
    if (len(channel_data) == 0):
        return dataRs
    for res in channel_data:
        channelId = res['id']
        location = res['location']
        time = res['time']
        key = channelId + '-' + location
        if dataRs.get(key) is None:
            dataRs[key] = {}
        dataRs[key][time] = res['value']
    return dataRs



def query_data(start,end,channel_conf,delete=False):
    #请求所有通道数据
    if start:
        tsdb_start = start
    else:
        tsdb_start = 1483200000000 #2017-01-01 00:00:00
    q="select * from m where time>{}ms and time<{}ms;".format(tsdb_start, end)
    response = conn_db.query(q, epoch='ms')
    dataRs = {}
    for channel_code in channel_conf.split(','):
        # channel_data_location1 = list(response.get_points(tags={'id': channel_code,'location':'1'}))
        # channel_data_location2 = list(response.get_points(tags={'id': channel_code,'location':'2'}))
        # dataRs = saveChannelData(dataRs, channel_data_location1)
        # dataRs = saveChannelData(dataRs, channel_data_location2)
        channel_data = list(response.get_points())
        dataDict = transformChannelDataFormat(channel_data)
        dataRs = dict(dataRs, ** dataDict)
    return dataRs


def query_last_data(channel_code):
    #请求某一通道最近的数据
    print('channel_code',channel_code)
    if channel_code is None:
        return {}
    q1="select last(*) from m where id='{}' and location='{}';".format(int(channel_code),'1')
    q2="select last(*) from m where id='{}' and location='{}';".format(int(channel_code),'2')
    responseLocation1 = conn_db.query(q1, epoch='ms')
    responseLocation2 = conn_db.query(q2, epoch='ms')
    channel_data_location1 = list(responseLocation1.get_points())
    channel_data_location2 = list(responseLocation2.get_points())
    if (len(channel_data_location1) > 0 and len(channel_data_location2) > 0):
        resxData = channel_data_location1[0]['last_value']
        resyData = channel_data_location2[0]['last_value']
        res = str(resxData) + ',' + str(resyData)
    return res


def delete_all_data_before(end):
    q="delete from m where time<{}ms;".format(end)
    conn_db.query(q, epoch='ms')
    return True