#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time

import sys
sys.path.append('../')
from functions import es_con
from functions import mysqlConf

def getItemUseRate(es,startTime,endTime):
	print "starting get the item use rate  ...."
	esBody = {"size":0,"query":{"filtered":{"query":{"query_string":{"query":"LogType:itemsLog","analyze_wildcard":True}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"gte":1469318400000,"lte":1469404800000,"format":"epoch_millis"}}}],"must_not":[]}}}},"aggs":{"2":{"terms":{"field":"level","size":280,"order":{"_term":"asc"}},"aggs":{"3":{"filters":{"filters":{"*":{"query":{"query_string":{"query":"*","analyze_wildcard":True}}},"item_id:1281":{"query":{"query_string":{"query":"item_id:1281","analyze_wildcard":True}}},"item_id:1282":{"query":{"query_string":{"query":"item_id:1282","analyze_wildcard":True}}},"item_id:1283":{"query":{"query_string":{"query":"item_id:1283","analyze_wildcard":True}}},"item_id:1284":{"query":{"query_string":{"query":"item_id:1284","analyze_wildcard":True}}},"item_id:1285":{"query":{"query_string":{"query":"item_id:1285","analyze_wildcard":True}}},"item_id:1286":{"query":{"query_string":{"query":"item_id:1286","analyze_wildcard":True}}},"item_id:1537":{"query":{"query_string":{"query":"item_id:1537","analyze_wildcard":True}}},"item_id:1538":{"query":{"query_string":{"query":"item_id:1538","analyze_wildcard":True}}},"item_id:1539":{"query":{"query_string":{"query":"item_id:1539","analyze_wildcard":True}}},"item_id:1540":{"query":{"query_string":{"query":"item_id:1540","analyze_wildcard":True}}},"item_id:1541":{"query":{"query_string":{"query":"item_id:1541","analyze_wildcard":True}}}}},"aggs":{"1":{"sum":{"field":"number"}}}}}}}}
	sTime = startTime*1000
	eTime = endTime*1000
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
       	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	result  = es.esSearch(esBody)
	getItemRate(result['aggregations']['2']['buckets'],startTime)
def getItemRate(result,startTime):
        conn = mysqlConf.connMysql()
        cursor = conn.cursor()
	for item in result:
		itemUseRate = {}
		all  = item['3']['buckets']['*']['1']['value']
		i_1281 = "%.2f" %(item['3']['buckets']['item_id:1281']['1']['value']/all*100)
		itemUseRate['1281'] = i_1281
		i_1282 = "%.2f" %(item['3']['buckets']['item_id:1282']['1']['value']/all*100)
		itemUseRate['1282'] = i_1282
		i_1283 = "%.2f" %(item['3']['buckets']['item_id:1283']['1']['value']/all*100)
		itemUseRate['1283'] = i_1283
		i_1284 = "%.2f" %(item['3']['buckets']['item_id:1284']['1']['value']/all*100)
		itemUseRate['1284'] = i_1284
		i_1285 = "%.2f" %(item['3']['buckets']['item_id:1285']['1']['value']/all*100)
		itemUseRate['1285'] = i_1285
		i_1286 = "%.2f" %(item['3']['buckets']['item_id:1286']['1']['value']/all*100)
		itemUseRate['1286'] = i_1286
		i_1537 = "%.2f" %(item['3']['buckets']['item_id:1537']['1']['value']/all*100)
		itemUseRate['1537'] = i_1537
		i_1538 = "%.2f" %(item['3']['buckets']['item_id:1538']['1']['value']/all*100)
		itemUseRate['1538'] = i_1538
		i_1539 = "%.2f" %(item['3']['buckets']['item_id:1539']['1']['value']/all*100)
		itemUseRate['1539'] = i_1539
		i_1540 = "%.2f" %(item['3']['buckets']['item_id:1540']['1']['value']/all*100)
		itemUseRate['1540'] = i_1540
		i_1541 = "%.2f" %(item['3']['buckets']['item_id:1541']['1']['value']/all*100)
		itemUseRate['1541'] = i_1541
		for k,v in itemUseRate.items():
			selectSql = "SELECT * FROM es_query_itemuse WHERE actTime='%d' AND level='%d' AND items='%s'" %(startTime,item['key'],k)
			sql = "INSERT INTO es_query_itemuse(level,actTime,items,itemRate) VALUES('%d','%d','%s','%s')" %(item['key'],startTime,k,v)  
			code = cursor.execute(selectSql)
			if(0 == code):
				cursor.execute(sql)
			else:
				print "the sql has been insert "
				continue
		conn.commit()
	conn.close()


