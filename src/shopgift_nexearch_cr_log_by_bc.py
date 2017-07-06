#!/bin/env python
#-*- coding:utf8 -*-


import getopt
import sys
import os 
import math
import re
from datetime import datetime
import time 


#################################################################
# main
#################################################################
bc_file = ""
opts, args = getopt.getopt(sys.argv[1:], 'd:')
for opt, arg in opts:
    if  opt == '-d':    bc_file= arg

usage = 'Usage ' + sys.argv[0] + '\n'
usage+= '\t -d <bc file> \n'

if not bc_file:
    sys.stderr.write(usage)
    sys.exit (1)

def main():
    bc_dic = read_bc(bc_file)

    for line in sys.stdin:
        line = line.rstrip("\n")
        arr = line.split("\t")

        if len(arr) < 5:
            continue 

        if len(arr) < 9 :
#            if arr[2] == "shopping":
#                print line 
            continue 

        query = arr[0]
        dtime = arr[1] 
        pid = arr[2] 
        sid = arr[3] 
        type = arr[4]
        rank = arr[5] 
        gdid = arr[6] 
        url = arr[7] 
        bc = arr[8].strip()

        ts=time.mktime(datetime.strptime(dtime, "%y/%m/%d %H:%M:%S").timetuple())
        if bc == "" or bc == "-": 
            continue 

        if bc in bc_dic:
            print "%s\t%.3f\tnexearch\t%s\t%s\t%s"%(bc, ts, query, gdid, url)

def read_bc (filename):
    bc_dic = {} 
    fp = file(filename)
    for line in fp:
        k,v = line.rstrip("\n").split("\t", 1)
        bc_dic[k] = v 
    fp.close()
    return bc_dic 

if __name__ == '__main__':
    main()

