#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time

def firstPassCount(es,startTime,endTime):
	print "starting get the first pass user  counts ...."
	esBody = { "size": 0, "query": { "filtered": { "query": { "query_string": { "query": "LogType:scoreLog AND counts:0", "analyze_wildcard": True } }, "filter": { "bool": { "must": [ { "range": { "actTime": { "gte": 0, "lte": 0} } } ], "must_not": [] } } } }, "aggs": { "2": { "terms": { "field": "level", "size": 280, "order": { "_term": "asc" } }, "aggs": { "1": { "cardinality": { "field": "uuid" } } } } } }
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
 	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	result  = es.esSearch(esBody)
	firstPassCount = firstPassCounts(result)
	return firstPassCount
	

def firstPassCounts(result):
	firstPassList =[0]*280
	for item in result['aggregations']['2']['buckets']:
		firstPassList[item['key']-1] = "%.2f" % item['1']['value']
	return firstPassList
