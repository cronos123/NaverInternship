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

class SearchItem:
    def __init__ (self):
        self.gdids= []
        self.query = ""

def main():
    curkey = ""

    prev_ts = 0.0
    prev_query = ""
    item = None
    items = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        if line.strip() == "":
            continue

        arr = line.split("\t")
        key = arr[0]

        if curkey != key :
            # 비쿠키가 다른 경우
            printItems(curkey, items)

            prev_ts = 0
            prev_query = ""
            item = None
            items = []

        try:
            ts=float(arr[1])
            domain = arr[2]
            query = arr[3]
            query_n = query.replace(" ", "").lower()
            gdid = arr[4].strip()
            url = ""
            if len(arr) > 5:
                url = arr[5]

            if query_n == "" or query_n == "-" or gdid == "" or gdid == "-":
                continue

            if query_n != prev_query:
                diff_ts = ts - prev_ts
                # 20분 이상 인터벌이 있는 경우 새로운 세션으로..
                if diff_ts > 60*20 :
                    printItems(curkey, items)
                    items = []

                item = SearchItem()
                item.query = query
                items.append(item)

            info = getInfo(domain ,gdid, url)
            if info != "":
                item.gdids.append(info)

            prev_ts = ts
            prev_query = query_n
        except:
            continue

        curkey = key
    printItems(curkey, items)


# url 및 gdid로부터 shopping/shoppingw 구분
def getInfo(domain, gdid, url):
    catid = ""
    nvmid = ""
    if "shopping.naver.com" in url:
        if domain == "shopping":
            if len(gdid) >= 10 and gdid.isdigit() :
                nvmid = gdid

        arr = url.split("?")
        if len(arr) > 1:
            for param in arr[1].split("&"):
                k, v = param.split("=", 1)
                if k == "cat_id":
                    catid = v
                elif k == "nv_mid":
                    if len(v) >= 10 and v.isdigit() :
                        nvmid = v
        if nvmid == "":
            return ""
        return "shopping:" + nvmid + ":" + catid

    if "shoppingw.naver.com" in url:
        nvmid = ""
        if len(gdid) >= 10 and gdid.isdigit() :
            nvmid = gdid
        else:
            v = url.split("?")[0].split("/")[-1]
            if len(v) >= 10 and v.isdigit() :
                nvmid = v
        if nvmid == "":
            return ""
        return "shopping"+":" + nvmid + ":shpwin"

    return domain +":" + gdid + ":"


# 쇼핑 관련 클릭이 있거나, 질의에 선물을 포함하는 경우만 남김
def printItems(bc, items):
    rets = []

    is_shop = False
    for i in range(len(items)):
        item = items[i]
        query = item.query

        gdids = ";".join(item.gdids)
        if "shopping" in gdids:
            is_shop = True
        if "선물" in query :
            is_shop = True

        rets.append( query + "" + gdids )

    if is_shop == False:
        return

    print bc +"\t"+ "\t".join(rets)

if __name__ == '__main__':
    main()

