"""
Copyright:tncet.com
Author:crayon
Date:2021-06-11
与采集箱通信程序
"""


import socket
import datetime
import time
import queue
import threading
import os
import configparser
import signal
from binascii import *
from tsdbutils import input_data
import numpy as np
import math as m

dir_root = os.path.dirname(os.path.abspath(__file__)) 
cf = configparser.ConfigParser()
cf.read(dir_root+'/config.cfg')
q = queue.Queue(maxsize=0)

def crc16Add(raw_cmd):
    #计算数据长度、地址码、命令字和数据域的和
    crc = int(raw_cmd[2:4],16) + int(raw_cmd[4:6],16) + int(raw_cmd[6:8],16) + int(raw_cmd[8:10],16)
    crc = hex(crc)
    crc = crc[2:].zfill(2)
    res = '{}{}'.format(raw_cmd,crc)
    return res

def run(work_status_num):
    #与采集箱通信发送指令
    try:
        while True:
            if work_status_num.value == 0:
                print('kill current pid')
                pid = os.getpid()
                os.kill(pid, signal.SIGKILL)
            s = socket.socket()
            tcp_address = (cf.get('dbox','ip'),int(cf.get('dbox','port'))) 
            s.connect(tcp_address)
            channel_code = cf.get('channel-conf','channel_code')
            channel_code = channel_code.split(',')
            for channel in channel_code:
                channel = hex(int(channel))[2:]
                channel = channel.upper()
                raw_cmd = '770500{:0>2}04'.format(channel)
                begin_sample_cmd = crc16Add(raw_cmd)
                s.send(bytes.fromhex(str(begin_sample_cmd)))
                time.sleep(0.04)
            s.close()
    except Exception as e:
        print(e)




def recv(work_status_num,target_period_num):
    #接收数据
    s = socket.socket()
    tcp_address = (cf.get('dbox','ip'),int(cf.get('dbox','port'))) 
    s.connect(tcp_address)
    s.settimeout(40)
    saveQdata_thread = threading.Thread(target=saveQdata,args=(target_period_num, ))
    saveQdata_thread.start()
    while True:
        try:
            if work_status_num.value == 0:
                print('kill current pid')
                pid = os.getpid()
                os.kill(pid, signal.SIGKILL)
            buff = s.recv(10240)
            response = buff
            responseList = response.split(b'w\x11')
            responseList = [x for x in responseList if x != b'']
            for response in responseList:
                # channelSignal = response[0:2].hex()
                if len(response) != 16:
                    print('数据发生错误',response.hex())
                    continue
                else:
                    dataDict = data_process(response)
                    q.put(dataDict)
        except Exception as e:
            print(e)

def saveQdata(target_period_num):
    #聚合与储存数据
    dataBufferList = []
    key = int(datetime.datetime.now().timestamp())
    while True:
        try:
            if q.empty():
                time.sleep(0.01)
                continue
            data = q.get()
            dataBufferList.append(data)
            #判断数据聚合在target_period时间中
            channel_code = cf.get('channel-conf','channel_code')
            channel_code = channel_code.split(',')
            target_period = target_period_num.value
            if (int(data['sampleTime'] / 1000) - key) < int(target_period):
                continue
            if len(dataBufferList) == 0:
                continue
            saveDataList = []
            for channel in channel_code:
                dataChannelDict = {}
                datax = []
                datay = []
                for dataDict in dataBufferList:
                    # print('dataDict',dataDict)
                    dataSDict = {}
                    if dataDict['channelId'] == int(channel):
                        datax.append(dataDict['xData'])
                        datay.append(dataDict['yData'])
                    dataChannelDict['sampleTime'] = dataDict['sampleTime']   
                if (len(datax) == 0 or len(datay) == 0):
                    continue
                dataChannelDict['channelId'] = channel 
                dataChannelDict['xData'] = np.around(np.average(datax), 5)
                dataChannelDict['yData'] = np.around(np.average(datay), 5)
                saveDataList.append(dataChannelDict)
            input_data(saveDataList)
            key = int(datetime.datetime.now().timestamp())
            dataBufferList = []
        except Exception as e:
            print(e)

        



def getSingleData(channel,work_status_num):
    #获取单通道数据
    s = socket.socket()
    tcp_address = (cf.get('dbox','ip'),int(cf.get('dbox','port'))) 
    s.connect(tcp_address)
    
    channel = hex(int(channel))[2:]
    channel = channel.upper()
    raw_cmd = '770500{:0>2}04'.format(channel)
    begin_sample_cmd = crc16Add(raw_cmd)
    s.send(bytes.fromhex(str(begin_sample_cmd)))
    s.settimeout(15)
    rawData = s.recv(10240)
    dataNewDict = {}
    dataDict = data_process(rawData)
    dataNewDict['1'] = dataDict['xData']
    dataNewDict['2'] = dataDict['yData']
    dataNewDict = str(dataNewDict)
    res = str(dataDict['xData']) + ',' + str(dataDict['yData'])
    return res





def data_process(data):
    #解析数据
    channelStartPoint = 1
    channelId = data[channelStartPoint - 1: channelStartPoint+ 1].hex()
    dataDict = {}
    xDataSymbol = data[channelStartPoint + 2:channelStartPoint+3].hex()
    xDataInt = data[channelStartPoint+ 3:channelStartPoint + 4].hex()
    xDataFloat = data[channelStartPoint+ 4:channelStartPoint+ 6].hex()
    xData = '{}.{}'.format(xDataInt,xDataFloat)
    xData = m.radians(float(xData))
    xData = round(xData,5)
    if xDataSymbol == '00':
        xData = float(xData)
    elif xDataSymbol == '10':
        xData = -float(xData)
    yDataSymbol = data[channelStartPoint + 6:channelStartPoint + 7].hex()
    yDataInt = data[channelStartPoint + 7:channelStartPoint + 8].hex()
    yDataFloat = data[channelStartPoint + 8:channelStartPoint + 10].hex()
    yData = '{}.{}'.format(yDataInt,yDataFloat)
    yData = m.radians(float(yData))
    yData = round(yData,5)
    if yDataSymbol == '00':
        yData = float(yData)
    elif yDataSymbol == '10':
        yData = -float(yData)
    dataDict['channelId'] = int(channelId, 16)
    dataDict['xData'] = xData
    dataDict['yData'] = yData
    dataDict['sampleTime'] = int(datetime.datetime.now().timestamp()*1000)
    return dataDict
