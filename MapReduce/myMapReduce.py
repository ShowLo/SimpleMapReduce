# -*- coding: UTF-8 -*-

import os
import sys
import ConfigParser
import subprocess
import json

#保存到分布式系统中去
def saveToDFS(fileName, master):
    subprocess.call(['scp', fileName, master + ':' + fileName])

#从分布式系统中下载文件
def loadFromDFS(fileName, destFileName, master):
    subprocess.call(['ssh', master, 'python', 'myJobTracker.py', 'load', fileName, destFileName])

def main(args):
    if len(args) < 2:
        print 'Error : Too less parameters!'
        sys.exit()
    commond = args[0]
    fileName = args[1] 
    #读入配置信息
    cf = ConfigParser.ConfigParser()
    cf.read('config.conf')
    master = cf.get('master', 'address')

    if commond == 'do':
        #将master的程序及配置文件发送到对应机器上去
        subprocess.call(['scp', 'myJobTracker.py', master + ':myJobTracker.py'])
        subprocess.call(['scp', 'myTaskTracker.py', master + ':myTaskTracker.py'])
        subprocess.call(['scp', 'myMapper.py', master + ':myMapper.py'])
        subprocess.call(['scp', 'myReducer.py', master + ':myReducer.py'])
        subprocess.call(['scp', 'config.conf', master + ':config.conf'])

        #保存到分布式系统中去
        saveToDFS(fileName, master)
        #reducer数量
        reducerNum = int(args[2])
        #启动MapReduce的计算
        subprocess.call(['ssh', master, 'python', 'myJobTracker.py', 'save', fileName, str(reducerNum)])
        
    elif commond == 'return':
        if len(args) < 3:
            print 'Not enough parameters for return function!'
            sys.exit()
        destFileName = args[2]
        #从分布式系统下载MapReduce计算结果到客户端
        loadFromDFS(os.path.splitext(fileName)[0] + '-result', destFileName, master)
        print 'save the result to ' + destFileName

if __name__ == '__main__':
    main(sys.argv[1:])