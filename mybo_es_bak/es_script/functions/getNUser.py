#!/usr/bin/env python 
#coding:utf-8
import MySQLdb as mysql
import datetime
import time
import sys


import mysqlConf
sys.path.append('../')
from log import logconf
import logging


def getAllInStall(es,startTime,endTime):
	print "start get all install "
        esBody = {"query":{"filtered": {"query":{"query_string": {"analyze_wildcard": True,"query": 'LogType:createUserLog'}},"filter": {"bool":{"must": [{"range": {"createtime": {"gte": 0,"lte": 0}}}],"must_not": []}}}},"aggs": {"1": {"cardinality": {"field": 'customerId'}}}}    
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['createtime']['gte'] =startTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['createtime']['lte'] = endTime
	result = es.esSearch(esBody)
	code = linkMysql(result,startTime)
	yesAllInStall = result['aggregations']['1']['value']
	allInStall =  code + yesAllInStall
	return allInStall
#链接mysql 把createtime 中的新用户插入mysql
def linkMysql(result,startTime):
        conn = mysqlConf.connMysql()
	cursor = conn.cursor()
	sql = "SELECT allInstall FROM es_query_timesummary WHERE actTime='%d'" %(startTime-86400)
	cursor.execute(sql)
	data = cursor.fetchone()
	if (None == data):
		data  = 0
	else:
		data = data[0]
	return data

	
