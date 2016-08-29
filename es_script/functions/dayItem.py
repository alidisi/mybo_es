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
def getDayItems(es,startTime,endTime):
	print " search day item use rate starting..... "
	esBody= { "size": 0, "query": { "filtered": { "query": { "query_string": { "query": "LogType:itemsLog", "analyze_wildcard": True } }, "filter": { "bool": { "must": [ { "range": { "@timestamp": { "gte": 1471996800000, "lte": 1472083200000, "format": "epoch_millis" } } } ], "must_not": [] } } } }, "aggs": { "2": { "filters": { "filters": { "*": { "query": { "query_string": { "query": "*", "analyze_wildcard": True } } }, "item_id:1281": { "query": { "query_string": { "query": "item_id:1281", "analyze_wildcard": True } } }, "item_id:1282": { "query": { "query_string": { "query": "item_id:1282", "analyze_wildcard": True } } }, "item_id:1283": { "query": { "query_string": { "query": "item_id:1283", "analyze_wildcard": True } } }, "item_id:1284": { "query": { "query_string": { "query": "item_id:1284", "analyze_wildcard": True } } }, "item_id:1285": { "query": { "query_string": { "query": "item_id:1285", "analyze_wildcard": True } } }, "item_id:1286": { "query": { "query_string": { "query": "item_id:1286", "analyze_wildcard": True } } }, "item_id:1537": { "query": { "query_string": { "query": "item_id:1537", "analyze_wildcard": True } } }, "item_id:1538": { "query": { "query_string": { "query": "item_id:1538", "analyze_wildcard": True } } }, "item_id:1539": { "query": { "query_string": { "query": "item_id:1539", "analyze_wildcard": True } } }, "item_id:1540": { "query": { "query_string": { "query": "item_id:1540", "analyze_wildcard": True } } }, "item_id:1541": { "query": { "query_string": { "query": "item_id:1541", "analyze_wildcard": True } } } } }, "aggs": { "1": { "sum": { "field": "number" } } } } } }
	#kibana 的@timestamp 为正常的时间戳*1000
	sTime = startTime*1000
        eTime = endTime*1000
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] = sTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	result = es.esSearch(esBody)
	insertMysql(result['aggregations']['2']['buckets'],startTime)


def insertMysql(result,startTime):
	conn = mysqlConf.connMysql()
        cursor = conn.cursor()
	itemRate = {'1281':{},'1282':{},'1283':{},'1284':{},'1285':{},'1286':{},'1537':{},'1538':{},'1539':{},'1540':{},'1541':{}}
	itemRate['1281']['rate'] = "%.2f" %(result['item_id:1281']['1']['value']/result['*']['1']['value']*100)
	itemRate['1281']['count'] = "%.d" %(result['item_id:1281']['1']['value'])
	itemRate['1282']['rate'] = "%.2f" %(result['item_id:1282']['1']['value']/result['*']['1']['value']*100)
	itemRate['1282']['count'] = "%.d" %(result['item_id:1282']['1']['value'])
	itemRate['1283']['rate'] = "%.2f" %(result['item_id:1283']['1']['value']/result['*']['1']['value']*100)
	itemRate['1283']['count'] = "%.d" %(result['item_id:1283']['1']['value'])
	itemRate['1284']['rate'] = "%.2f" %(result['item_id:1284']['1']['value']/result['*']['1']['value']*100)
	itemRate['1284']['count'] = "%.d" %(result['item_id:1284']['1']['value'])
	itemRate['1285']['rate'] = "%.2f" %(result['item_id:1285']['1']['value']/result['*']['1']['value']*100)
	itemRate['1285']['count'] = "%.d" %(result['item_id:1285']['1']['value'])
	itemRate['1286']['rate'] = "%.2f" %(result['item_id:1286']['1']['value']/result['*']['1']['value']*100)
	itemRate['1286']['count'] = "%.d" %(result['item_id:1286']['1']['value'])
	itemRate['1537']['rate'] = "%.2f" %(result['item_id:1537']['1']['value']/result['*']['1']['value']*100)
	itemRate['1537']['count'] = "%.d" %(result['item_id:1537']['1']['value'])
	itemRate['1538']['rate'] = "%.2f" %(result['item_id:1538']['1']['value']/result['*']['1']['value']*100)
	itemRate['1538']['count'] = "%.d" %(result['item_id:1538']['1']['value'])
	itemRate['1539']['rate'] = "%.2f" %(result['item_id:1539']['1']['value']/result['*']['1']['value']*100)
	itemRate['1539']['count'] = "%.d" %(result['item_id:1539']['1']['value'])
	itemRate['1540']['rate'] = "%.2f" %(result['item_id:1540']['1']['value']/result['*']['1']['value']*100)
	itemRate['1540']['count'] = "%.d" %(result['item_id:1540']['1']['value'])
	itemRate['1541']['rate'] = "%.2f" %(result['item_id:1541']['1']['value']/result['*']['1']['value']*100)
	itemRate['1541']['count'] = "%.d" %(result['item_id:1541']['1']['value'])
	for k,v in itemRate.items():
		dateTime = time.localtime(startTime)
	        dateTime = time.strftime("%Y-%m-%d %H:%M:%S",dateTime)
		selectsql = "SELECT * FROM es_query_itemday where actTime='%d' AND items='%s'" %(startTime,k)
		sql = "INSERT INTO es_query_itemday(actTime,items,dateTime,itemRate,itemNumbers) VALUES('%d','%s','%s','%s','%s')" %(startTime,k,dateTime,v['rate'],v['count'])
		code = cursor.execute(selectsql)
		if(0 == code):
			cursor.execute(sql)
			conn.commit()
		else:
			print "the sql has been inserted "
	conn.close() 
