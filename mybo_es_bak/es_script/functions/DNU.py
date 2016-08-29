#!/usr/bin/env python 
#coding:utf-8
import es_con

def DNU(es,startTime,endTime):
#	index = "log-business-*"
#	es = es_con.esClass(index)
#	(startTime,endTime) = es.dayTimeStamp(15)
	esBody = {"query":{"filtered": {"query":{"query_string": {"analyze_wildcard": True,"query": 'LogType:createUserLog'}},"filter": {"bool":{"must": [{"range": {"createtime": {"gte": 0,"lte": 0}}}],"must_not": []}}}},"aggs": {"1": {"cardinality": {"field": 'customerId'}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['createtime']['gte'] =startTime
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['createtime']['lte'] = endTime
	result = es.esSearch(esBody)
	return result['aggregations']['1']['value']
