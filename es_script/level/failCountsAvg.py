#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time




def failAvg_counts(es,startTime,endTime):
	print "starting get the TOP  level...."
	esBody={"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:scoreLog AND NOT win:(1 OR 5)'}},"filter": {"bool": {"must": [{"range": {"actTime": {"gte": 0,"lte": 0}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}},"aggs": {"1": {"cardinality":{"field": "customerId"}}}}}}
	esBody1={"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:scoreLog AND NOT win:(1 OR 5)'}},"filter": {"bool": {"must": [{"range": {"actTime": {"gte": 0,"lte": 0}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	user = es.esSearch(esBody)
	failAvg = getFailAvg(user['aggregations']['2']['buckets'])
	return failAvg
def  getFailAvg(counts):
	userList = [0]*280
	countsList = [0]*280
	result = [0]*280
	for item in counts:
		userList[item['key']-1] = item['1']['value']
		countsList[item['key'] -1] = item['doc_count']
	bb = zip(userList,countsList)
	i = 0
	for user,win in bb:
		user = float(user)
		win = float(win)
		if (0 == user):
			result[i] = 0
			i+=1
		else:
			avg  = "%.2f" %(win/user)
			result[i]=avg
			i+=1
	return result
