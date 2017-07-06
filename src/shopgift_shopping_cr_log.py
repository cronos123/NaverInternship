#!/bin/env python
#-*- coding:utf8 -*-

import getopt
import sys
import os 
import math
import re
import urllib
from datetime import datetime
import time 

def main():
    for line in sys.stdin:
        line = line.rstrip("\n")
        arr = line.split("\t")

        ip = arr[0]
        ts = arr[1]
        gdid = arr[5]
        bc = arr[8].strip()
        url = arr[11]
        query = arr[22]

        query = urllib.unquote(query).replace("\t", " ") 
        url = urllib.unquote(url).replace("\t", "") 

        if bc == "" or bc == "-":
            continue 
        print "%s\t%s\tshopping\t%s\t%s\t%s"%(bc, ts, query, gdid, url)

if __name__ == '__main__':
    main()

