#!/usr/bin/env python 
#coding:utf-8
from __future__ import division
import MySQLdb as mysql
import datetime
import time
import sys

import mysqlConf
sys.path.append('../')
from log import logconf
import logging


#从es返回所有符合条件的值的原始字段
def income(es,startTime,endTime):
	print " search iapLog for packetage   starting..... "
	esBody = {"fields":["item","channel","map_version","map_name","e_time","uuid"],"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:iapLog'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}}}
	#kibana 的@timestamp 为正常的时间戳*1000
	sTime = startTime*1000
        eTime = endTime*1000
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	result = es.esSearch(esBody)
	insertPackage(result,startTime)
	success = 1
	return success
def	insertPackage(result,startTime):
	conn = mysqlConf.connMysql()
	cursor = conn.cursor()
        print "insert item packetage  to ....."
        for item in result:
		selectExit = 0
		selectsql = "SELECT * FROM es_query_itemPackage  WHERE uuid='%s' AND eTime='%s' AND item='%s'" %(item['uuid'][0],item['e_time'][0],item['item'][0])
		sql="INSERT INTO es_query_itemPackage(item,channel,map_version,map_name,eTime,uuid,actTime) VALUES('%s','%s','%s','%s','%s','%s','%d')"  %(item['item'][0],item['channel'][0],item['map_version'][0],item['map_name'][0],item['e_time'][0],item['uuid'][0],startTime)
		selectExit = cursor.execute(selectsql)
		if (0==selectExit):
			try:		
				cursor.execute(sql)
			except mysql.Error,e:
                        	 conn.rollback()
                        	 print "rollback",e
	conn.commit()
	conn.close()
def	getIncome(startTime):
	conn = mysqlConf.connMysql()
	cursor = conn.cursor()
	itemList = {'l_gold_coins_package':19.99,'xl_gold_coins_package':49.99,'xs_combination_package':0.99,'xs_gold_coins_package':0.99,'s_gold_coins_package':4.99,'m_gold_coins_package':9.99}
	income = 0
	for (k,v) in itemList.items():
		sql = "SELECT COUNT(*)  FROM es_query_itemPackage WHERE item='%s' AND actTime=%d " %(k,startTime)
		try:
			cursor.execute(sql)
			data = cursor.fetchone()
			income +=data[0]*v
		except mysql.Error,e:
                         conn.rollback()
                         print "rollback",e
	return income
