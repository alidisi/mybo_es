#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time




def getStepAvg(es,startTime,endTime): 
	"starting get the step average ...."
	esBody={"size":0,"query":{"filtered": { "query":{"query_string":{"query": "LogType:scoreLog AND win:1", "analyze_wildcard": True}},"filter":{"bool":{"must":[{"range":{"actTime":{"gte": 0, "lte": 0}}}],"must_not":[]}}}}, "aggs": {"2":{"terms":{"field":"level","size":280, "order":{"_term": "asc"}},"aggs":{"1": {"avg": {"field": "step"}}}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	result = es.esSearch(esBody)
	stepAvg = getAvg(result['aggregations']['2']['buckets'])
	return stepAvg
def getAvg(result):
	stepAvgList = [0]*280
	for item in result:
		avg = "%.2f" %item['1']['value']
		stepAvgList[item['key']-1] = avg
	return stepAvgList
