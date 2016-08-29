#!/usr/bin/env python 
#coding:utf-8
import es_con
import time

def PU(es,startTime,endTime):
#	index = "log-business-*"
#	es = es_con.esClass(index)
#	(startTime,endTime) = es.dayTimeStamp(15)
	startTime = startTime*1000
	endTime = endTime*1000
#	print start1Time,end1Time
	esBody = {"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:iapLog'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}},"aggs": {"1": {"cardinality": {"field": 'uuid'}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =startTime
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = endTime
	print esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte']
	print esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte']
	result = es.esSearch(esBody)
	return result['aggregations']['1']['value']
