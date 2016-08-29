#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time

def getTryCount(es,startTime,endTime):
	print "starting get the item counts ...."
	esBody={ "size": 0, "query": { "filtered": { "query": { "query_string": { "query": "LogType:scoreLog AND isFirstPass:1", "analyze_wildcard": True } }, "filter": { "bool": { "must": [ { "range": { "actTime": { "gte": 1472436069070, "lte": 1472436969071} } } ], "must_not": [] } } } }, "aggs": { "2": { "terms": { "field": "level", "size": 280, "order": { "_term": "asc" } }, "aggs": { "1": { "avg": { "field": "counts" } } } } } }
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
 	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	result  = es.esSearch(esBody)
	tryCount = tryCounts(result)
	return tryCount

def tryCounts(result):
	tryCountsList =[0]*280
	for item in result['aggregations']['2']['buckets']:
		tryCountsList[item['key']-1] = "%.2f" % item['1']['value']
	return tryCountsList
