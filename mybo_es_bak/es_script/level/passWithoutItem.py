#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time




def passWithoutItem_counts(es,startTime,endTime):
	print "starting get the pass rate without item ...."
	esBody={"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:scoreLog AND items_count:0'}},"filter": {"bool": {"must": [{"range": {"actTime": {"gte": 0,"lte": 0}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}}}}}
	esBody1={"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:scoreLog AND win:1 AND items_count:0'}},"filter": {"bool": {"must": [{"range": {"actTime": {"gte": 0,"lte": 0}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	esBody1['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
        esBody1['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	user = es.esSearch(esBody)
	winUser = es.esSearch(esBody1)
	passWithoutItem = getPassWithoutItemRate(user['aggregations']['2']['buckets'],winUser['aggregations']['2']['buckets'])
	return passWithoutItem
def getPassWithoutItemRate(user,winUser):
	userList = [0]*280
	winUserList = [0]*280
	result = [0]*280
	for item in user:
		userList[item['key']-1] = item['doc_count']
	for item in winUser:
		winUserList[item['key']-1] = item['doc_count']
	bb = zip(userList,winUserList)
	i = 0
	for user,win in bb:
		user = float(user)
		win = float(win)
		if (0 == user):
			result[i] = 0
			i+=1
		else:
			rate = "%.2f" %(win/user*100)
			result[i]=rate
			i+=1
	return  result
