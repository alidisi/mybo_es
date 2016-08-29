#!/usr/bin/env python 
#coding:utf-8
import MySQLdb as mysql
import datetime
import time

import mysqlConf
#从es返回所有符合条件的值的原始字段
def apuCounts(es,startTime,endTime):
	print " insert APU starting..... "
	esBody = {"fields":["uuid"],"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:scoreLog'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}}}
	#kibana 的@timestamp 为正常的时间戳*1000
	sTime = startTime*1000
        eTime = endTime*1000
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	result = es.esSearch(esBody)
	dau_set = set([])
	for item in result:
		dau_set.add(item['uuid'][0])
	print "the dau_set len is: %d" %len(dau_set)
	allPU = getAllPU()
	print "the allPU len is :%d" %len(allPU)
	apuset = dau_set & allPU
	apuCounts = len(apuset)
	return apuCounts
	
def	getAllPU():
#	conn = mysql.connect(host='127.0.0.1',user='mybodev',passwd='mybo-dev',db='test')
	conn = mysqlConf.connMysql()
	cursor = conn.cursor()
	sql = 'SELECT uuid FROM es_query_allPU'
	cursor.execute(sql)
	data = cursor.fetchall()
	allPU = set([])
	for item in data:
		allPU.add(item[0])
	return  allPU
