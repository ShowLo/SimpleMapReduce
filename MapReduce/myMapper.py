# -*- coding: UTF-8 -*-

import os
import sys
import json

#mapper函数
def mapper(record):
    mapperResult = {record : 1}
    return mapperResult

#同时进行combiner和shuffle
def combinerAndShuffle(mapperResultList, reducerNum):
    combinerAndShuffleResult = [{} for i in range(reducerNum)]
    for mapperResult in mapperResultList:
        #取出mapper的结果
        (k, v), = mapperResult.items()
        #按照key值分为多份，份数与reducer的个数相同
        index = hash(k) % reducerNum
        #相同key值合并
        if k in combinerAndShuffleResult[index]:
            combinerAndShuffleResult[index][k].append(v)
        else:
            combinerAndShuffleResult[index][k] = [v]
    return combinerAndShuffleResult

        

def main(args):
    #需要处理的分块文件路径
    blockFilePath = args[0]
    reducerNum = int(args[1])
    #对分块文件的各行分别应用mapper函数
    mapperResultList = []
    print 'start Mapper for block file : ' + blockFilePath + '...'
    with open(blockFilePath, 'r') as f:
        for line in f:
            mapperResultList.append(mapper(line.strip()))
    #将对分块文件应用mapper函数的之后得到的结果再进行combine和shuffle
    combinerAndShuffleResult = combinerAndShuffle(mapperResultList, reducerNum)
    #将得到的中间结果写入磁盘
    (folder, fileName) = os.path.split(blockFilePath)
    (realFileName, extension) = os.path.splitext(fileName)
    #放在中间文件夹中
    middleFolder = 'middle-' + '-'.join(realFileName.split('-')[:-1])
    if not os.path.exists(middleFolder):
        os.mkdir(middleFolder)
    for i in range(reducerNum):
        middleResult = combinerAndShuffleResult[i]
        middleFileName = 'middle-' + str(i) + '-' + fileName + '.json'
        #经过mapper、combiner、shuffle得到的中间文件(eg:middle-number/middle-0-number.txt.json)
        with open(os.path.join(middleFolder, middleFileName), 'w') as f:
            json.dump(middleResult, f)

if __name__ == '__main__':
    main(sys.argv[1:])