import signal
import os
import configparser
import multiprocessing
import socket
import data_box
import time
import datetime
import tsdbutils
import threading
import json

def auto_clean_data_before():
    while True:
        time.sleep(86400)
        try:
            # start = int(datetime.datetime.strptime('2017-01-01','%Y-%m-%d').timestamp())*1000
            current_time = datetime.datetime.now()
            end = int((current_time - datetime.timedelta(days = 30)).timestamp())*1000
            tsdbutils.delete_all_data_before(end)
            print('数据已清理成功')
        except Exception as e:
            print('清理数据异常：',e)

def clean_data():
    try:
        # start = int(datetime.datetime.strptime('2017-01-01','%Y-%m-%d').timestamp())*1000
        current_time = datetime.datetime.now()
        end = int(current_time.timestamp())*1000
        tsdbutils.delete_all_data_before(end)
        print('数据已清理成功')
    except Exception as e:
        print('清理数据异常：',e)


def sigHandler(sig_num, addtion):
    print("aaa:", sig_num)
    os.killpg(os.getpgid(os.getpid()), signal.SIGKILL)


if __name__ == '__main__':
    
    signal.signal(signal.SIGTERM, sigHandler)
    
    #配置文件地址
    dir_root = os.path.dirname(os.path.abspath(__file__)) 
    cf = configparser.ConfigParser()
    cf.read(dir_root+'/config.cfg')
    
    # target down sample period
    target_period_num = multiprocessing.Value('i', int(cf.get('dbox','target_period')))

    channel_code = cf.get('channel-conf','channel_code')
    channel_code = channel_code.split(',')

    #开启定期清理数据线程
    clean_thread = threading.Thread(target=auto_clean_data_before,args=())
    clean_thread.setDaemon(True)
    clean_thread.start()

    #绑定tcp地址，开始监听
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_address = (cf.get('ic-address','tcp_ip'),int(cf.get('ic-address','tcp_port'))) #工控机tcp地址
    s.bind(tcp_address)
    s.listen()
    dbox_thread = None


    work_status_num = multiprocessing.Value('i', int(cf.get('dbox','work_status')))
    if (work_status_num.value == 1):
        dbox_thread = multiprocessing.Process(target=data_box.run,args=(work_status_num, ))
        dbox_thread.daemon = True
        dbox_thread.start()
        dbox_recv = multiprocessing.Process(target=data_box.recv,args=(work_status_num,target_period_num))
        dbox_recv.start()


    # 主线程持续监听
    while True:
        try:
            c = s.accept()[0]
            print('已经监听到来自服务器的连接：',c)
            cmd = c.recv(1024).decode('utf-8')
            print('来自服务器的命令：',cmd)
            cmd_head = cmd.split(':')[0]
            print('cmd_head', cmd_head)
            #cmd01 开始采样
            if cmd_head == '01': 
                '''
                '''
                try:
                    if work_status_num.value == 1:
                        res = res = '01:0:'
                    else:
                        print('开启采集进程')

                        work_status_num.value = 1
                        cf.set('dbox','work_status', str(work_status_num.value))
                        o = open('config.cfg','w')
                        cf.write(o)
                        o.close()

                        dbox_thread = multiprocessing.Process(target=data_box.run,args=(work_status_num, ))
                        dbox_thread.daemon = True
                        dbox_thread.start()
                        dbox_recv = multiprocessing.Process(target=data_box.recv,args=(work_status_num,target_period_num))
                        dbox_recv.daemon = True
                        dbox_recv.start()

                        res = '01:1:'
                except:
                    res = '01:0:'
            
            #cmd02 结束采样
            elif cmd_head == '02':
                '''
                '''
                try:
                    if work_status_num.value == 1:
                        # os.kill(dbox_thread.pid,signal.SIGKILL)
                        # os.kill(dbox_recv.pid,signal.SIGKILL)
                        dbox_thread = None
                        dbox_recv = None
                        work_status_num.value = 0
                        cf.set('dbox','work_status', str(work_status_num.value))
                        o = open('config.cfg','w')
                        cf.write(o)
                        o.close()
                    res = '02:1:'
                except:
                    res = '02:0:'

            #cmd03 查询通道配置
            elif cmd_head == '03':
                '''
                '''
                try:
                    channel_conf = cf.get('channel-conf','channel_code')
                    if channel_conf != '':
                        res = '03:1:' + channel_conf
                    else:
                        res = '03:0:'
                except:
                    res = '03:0:'

            #cmd04 返回单通道数据
            elif cmd_head == '04': 
                '''
                '''
                try:
                    channel_code = cmd.split(':')[1].split('-')[0]
                    start = cmd.split(':')[1].split('-')[1]
                    end = cmd.split(':')[1].split('-')[2]
                    datas = tsdbutils.query_single_data(start, end, channel_code)
                    if datas is not None:
                        res = json.dumps(datas)
                    else:
                        print('datas is None')
                        res = '04:0:'
                except:
                    print('error')
                    res = '04:0:'

            #cmd05 查询采样状态
            elif cmd_head == '05': 
                '''
                '''
                if dbox_thread is not None and dbox_recv is not None:
                    res = '05:1:'
                else:
                    res = '05:0:'
            #cmd06 查询采样间隔
            elif cmd_head == '06': 
                '''
                '''
                try:
                    target_period = cf.get('dbox','target_period')
                    if target_period != '':
                        res = '06:1:' + target_period
                    else:
                        res = '06:0:'
                except:
                    res = '06:0:'
            #cmd07 设置采样间隔
            elif cmd_head == '07':
                '''
                '''
                try:    
                    target_period = cmd.split(':')[1]
                    cf.set('dbox','target_period',target_period)
                    target_period_num.value = int(target_period)
                    o = open('config.cfg','w')
                    cf.write(o)
                    o.close()
                    res = '07:1:'
                except Exception as e:
                    res = '07:0:'

            #cmd08 批量返回通道数据
            elif cmd_head == '08': 
                '''
                '''
                try:
                    channel_conf = cf.get('channel-conf','channel_code')
                    start = cmd.split(':')[1].split('-')[0]
                    end = cmd.split(':')[1].split('-')[1]
                    print(start,'start',end,'end','channel_conf',channel_conf)
                    datas = tsdbutils.query_data(start,end,channel_conf)
                    print('datas',datas)
                    if datas is not None:
                        res = json.dumps(datas)
                    else:
                        res = '08:0:'
                except Exception as e:
                    print('err:', e)
                    res = '08:0:'

            #cmd10 清空数据
            elif cmd_head == '10':
                '''
                '''
                try:
                    clean_data(channel_code)
                    res = '10:1:'
                except Exception as e:
                    print('err', e)
                    res = '10:0:'

            #cmd11 读取通道配置        
            elif cmd_head == '11':
                try:
                    channel_code = cmd.split(':')[1]
                    cf.set('channel-conf','channel_code', str(channel_code))
                    o = open('config.cfg','w')
                    cf.write(o)
                    o.close()
                    res = '11:1:'
                except Exception as e:
                    print('err:', e)
                    res = '11:0:'
                    
            #cmd12 手动测量
            elif cmd_head == '12':
                try:
                    channel_code = cmd.split(':')[1].split('-')[0]
                    datas = tsdbutils.query_last_data(channel_code)
                    if datas is not None:
                        res = json.dumps(datas) + '\r\n'
                    else:
                        res = '12:0:\r\n'
                except Exception as e:
                    print('err:', e)
                    res = '12:0:\r\n'

            print('返回给服务器的数据：',res)
            res += '\r\n'
            c.send(res.encode('utf-8'))
            time.sleep(1)
        except Exception as e:
            print('主线程异常',e)
            time.sleep(1)
            c.close()
