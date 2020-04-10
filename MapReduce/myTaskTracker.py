# -*- coding: UTF-8 -*-

import os
import random
import ConfigParser
import sys
import json
import subprocess

#将文件发送到master中去
def loadToMaster(block_fileName, block_dir, master):
    subprocess.call(['scp', os.path.join(block_dir, block_fileName), master + ':' + os.path.join(block_dir, block_fileName)])

#如果文件夹不存在则新建文件夹
def createFolderIfNotExist(folder):
    if not os.path.exists(folder):
        os.mkdir(folder)

#发送文件分块到其他的slave上去
def saveToOtherSlave(filePath, destFilePath, slave):
    subprocess.call(['scp', filePath, slave + ':' + destFilePath])

#从其他slave下载文件(eg:number-result-block/number-result-0000)
def loadFromOtherSlave(filePath):
    (block_dir, fileName) = os.path.split(filePath)
    #源文件名(eg:number-result)
    original_fileName = '-'.join(fileName.split('-')[:-1])
    if not os.path.exists(block_dir):
        os.mkdir(block_dir)
    #读入配置信息
    cf = ConfigParser.ConfigParser()
    cf.read('config.conf')
    master = cf.get('master', 'address')
    selfAddr = cf.get('self', 'address')
    #需要从master找到有此文件的slave
    process = subprocess.Popen(['ssh', master, 'python', 'myJobTracker.py', 'slaveExistFile', fileName], stdout = subprocess.PIPE)
    slave = (process.stdout.read()).strip()
    #然后再直接从此slave中请求需要的文件分块
    subprocess.call(['ssh', slave, 'python', 'myTaskTracker.py', 'save', fileName, block_dir, selfAddr])
    #请求完之后将当前slave地址加入到所请求分块的slave信息中去
    subprocess.call(['ssh', master, 'python', 'myJobTracker.py', 'addSlaveToFile', fileName, selfAddr])

def startMapper(fileName, reducerNum):
    subprocess.call(['python', 'myMapper.py', fileName, str(reducerNum)])

def startReducer(fileName, reducerCount, master, selfAddr):
    subprocess.call(['python', 'myReducer.py', fileName, str(reducerCount), master, selfAddr])

#删除文件或者文件夹
def deleteMiddleFile(fileName):
    if os.path.exists(fileName):
        if os.path.isfile(fileName):
            os.remove(fileName)
        else:
            for f in os.listdir(fileName):
                os.remove(os.path.join(fileName,f))
            os.removedirs(fileName)

#保存分块文件时，如果此分块文件夹已存在，删除之前的分块，否则新建分块文件夹
def deleteBlockFilesOrCreateBlock(block_dir):
    #放置文件分块的文件夹是否存在
    if not os.path.exists(block_dir):
        #不存在的话新建文件夹
        os.mkdir(block_dir)
    else:
        #存在的话需要清空文件夹里面的分块
        for f in os.listdir(block_dir):
            os.remove(os.path.join(block_dir, f))

def main(args):
    if len(args) < 2:
        print 'Not enough parameters for myTaskTracker.py!'
        sys.exit()
    functionName = args[0]
    
    #读入配置信息
    cf = ConfigParser.ConfigParser()
    cf.read('config.conf')
    master = cf.get('master', 'address')

    fileName = args[1]
    if functionName == 'save':
        block_dir = args[2]
        #自己的地址
        slave = args[3]
        #将分块发送到其他slave中去
        saveToOtherSlave(os.path.join(block_dir, fileName), os.path.join(block_dir, fileName), slave)
    elif functionName == 'load':
        block_dir = args[2]
        #将分块数据发送到master中去
        loadToMaster(fileName, block_dir, master)
    elif functionName == 'loadFromOtherSlave':
        filePath = args[1]
        loadFromOtherSlave(filePath)
    elif functionName == 'startMapper':
        reducerNum = int(args[2])
        #开启mapper
        startMapper(fileName, reducerNum)
    elif functionName == 'startReducer':
        reducerCount = int(args[2])
        selfAddr = cf.get('self', 'address')
        #开启reducer
        startReducer(fileName, reducerCount, master, selfAddr)
    elif functionName == 'deleteMiddleFile':
        deleteMiddleFile(fileName)
    elif functionName == 'createFolderIfNotExist':
        createFolderIfNotExist(args[1])
    elif functionName == 'deleteBlockFilesOrCreate':
        deleteBlockFilesOrCreateBlock(args[1])

if __name__ == '__main__':
    main(sys.argv[1:])