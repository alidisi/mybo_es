#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time

import sys
sys.path.append('../')
from functions import es_con
from functions import mysqlConf

def getExitRate(es,startTime,endTime):
	print "starting get the item use rate  ...."
	esBody = {"size":0,"query":{"filtered":{"query":{"query_string":{"query":"LogType:scoreLog","analyze_wildcard":True}},"filter":{"bool":{"must":[{"range":{"actTime":{"gte":1469318400000,"lte":1469404800000,"format":"epoch_millis"}}}],"must_not":[]}}}},"aggs":{"2":{"terms":{"field":"level","size":280,"order":{"_term":"asc"}},"aggs":{"3":{"filters":{"filters":{"*":{"query":{"query_string":{"query":"*","analyze_wildcard":True}}},"win:2":{"query":{"query_string":{"query":"win:2","analyze_wildcard":True}}},"win:3":{"query":{"query_string":{"query":"win:3","analyze_wildcard":True}}},"win:4":{"query":{"query_string":{"query":"win:4","analyze_wildcard":True}}}}}}}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
       	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	result  = es.esSearch(esBody)
	getItemRate(result['aggregations']['2']['buckets'],startTime)
def getItemRate(items,startTime):
        conn = mysqlConf.connMysql()
        cursor = conn.cursor()
	for item in items:
		all  = item['3']['buckets']['*']['doc_count']
		win_2 = item['3']['buckets']['win:2']['doc_count']
		win_3 = item['3']['buckets']['win:3']['doc_count']
		win_4 = item['3']['buckets']['win:4']['doc_count']
		totalExit = win_2 + win_3 +win_4
		exitRate = "%.2f" %(float(totalExit)/float(all) *100)
		manuallyExit = win_2 + win_4
		selectSql = "SELECT * FROM es_query_exit  WHERE actTime='%d' AND level='%d'" %(startTime,item['key'])
		sql = "INSERT INTO es_query_exit(level,actTime,totalExit,playCounts,exitRate,manuallyExit,restart) VALUES('%d','%d','%d','%d','%s','%d','%d')" %(item['key'],startTime,totalExit,all,exitRate,manuallyExit,win_3)  
		code = cursor.execute(selectSql)
		if(0 == code):
			cursor.execute(sql)
		else:
			print "the sql has been insert "
			pass
	conn.commit()
	conn.close()
