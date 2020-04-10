# -*- coding: UTF-8 -*-

import os
import sys

def main(args):
    filePath = args[0]
    with open(filePath, 'r') as f:
        numSum1 = 0
        numSum2 = 0
        numCount = 0
        for line in f:
            result = line.split(',')
            numSum1 += float(result[0])
            numSum2 += float(result[1])
            numCount += int(result[2])
        mean = numSum1 / numCount
        variance = numSum2 / numCount - mean**2
        print 'mean = %.5f' % mean
        print 'variance = %.5f' % variance

if __name__ == '__main__':
    main(sys.argv[1:])