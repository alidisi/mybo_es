#!/usr/bin/env python 
#coding:utf-8

import es_con
import MySQLdb as mysql
import datetime
import time


import mysqlConf

#从es返回所有符合条件的值的原始字段
def getPU(es,startTime,endTime):
	print " insert PU starting..... "
	esBody = {"fields":["uuid","channel","map_version","map_name"],"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:iapLog'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}}}
	#kibana 的@timestamp 为正常的时间戳*1000
	sTime = startTime*1000
        eTime = endTime*1000
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	result = es.esSearch(esBody)
	linkMysql(result,startTime)
#PU 不要自己吧uuid都拉出来在自己统计一下 这个函数这做把今天的PU取出来 以前没有的PU的uuid 插入mysql 
#	apu_set = set([])
#	for item in result:
#		apu_set.add(item['uuid'][0])
#	puCounts = len(apu_set)
	success = 1
	return success
	
def linkMysql(result,startTime):
#	conn = mysql.connect(host='127.0.0.1',user='mybodev',passwd='mybo-dev',db='test')
	conn = mysqlConf.connMysql()
	cursor = conn.cursor()
	dateTime = time.localtime(startTime)
	dateTime = time.strftime("%Y-%m-%d %H:%M:%S",dateTime)
	print "insert PU to allPU....."
	for item in result:
		commit = True
		selectsql = "SELECT * FROM es_query_allPU  WHERE uuid='%s'" %item['uuid'][0]
		sql = "INSERT INTO es_query_allPU(actTime,uuid,channel,dateTime,mapName,mapVersion) VALUES('%d','%s','%s','%s','%s','%d')" %(startTime,item['uuid'][0],item['channel'][0],dateTime,item['map_name'][0],item['map_version'][0])
		try:
			uuid_exit = cursor.execute(selectsql)
			if (0 == uuid_exit):
				cursor.execute(sql)
		except mysql.Error,e:
			conn.rollback()
			commit = False
			print "rollback",e
		if(commit):
			conn.commit()
	conn.close()
def get_counts(sequence):
	counts = {}
	for x in sequence:
		if x in counts:
			counts[x] +=1
		else:
			counts[x] = 1
	return counts
	print "sql insert successful"
def getNPUCounts(startTime):
#	conn = mysql.connect(host='127.0.0.1',user='mybodev',passwd='mybo-dev',db='test')
	conn = mysqlConf.connMysql()
	cursor = conn.cursor()
	sql = 'SELECT COUNT(*) FROM es_query_allPU WHERE actTime =%d' %startTime
	cursor.execute(sql)
	data = cursor.fetchone()
	npuCounts = data[0]
	return npuCounts
