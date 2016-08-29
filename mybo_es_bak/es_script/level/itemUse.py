#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time

'''
start 为前置道具  run 为普通道具
["1541"] = "start"
["1539"] = "start",
["1285"] = "start",
["1281"] = "run",
["1282"] = "run",
["1283"] = "run",
["1284"] = "run",
["1286"] = "run" ,
["1537"] = "run",
["1538"] = "run" ,
["1540"] = "run",
'''

class item(object):
	def __init__(self,es,startTime,endTime):
		self.es =es 
		self.startTime = startTime
		self.endTime = endTime
		self.getItem_counts(self.es,self.startTime,self.endTime)
		self.getItemUseRate()
	def getItem_counts(self,es,startTime,endTime):
		print "starting get the item counts ...."
		esBody={"size":0,"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:scoreLog'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}}}}}
		esBody1={"size":0,"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:itemsLog  AND  item_id:(1541  OR  1539  OR  1285)'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}},"aggs":{"1":{"cardinality": {"field": "logId"}}}}}}
		esBody2={"size":0,"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:itemsLog  AND  NOT  item_id:(1541  OR  1539  OR  1285)'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}},"aggs":{"1":{"cardinality": {"field": "logId"}}}}}}
		esBody3={"size":0,"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:itemsLog  AND  NOT  item_id:(1541  OR  1539  OR  1285)'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}},"aggs":{"1":{"cardinality": {"field": "customer_id"}}}}}}
		esBody4={"size":0,"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:itemsLog  AND  NOT  item_id:(1541  OR  1539  OR  1285)'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}},"aggs":{"1":{"sum": {"field": "number"}}}}}}
		sTime = startTime*1000
		eTime = endTime*1000
		esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
       		esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
		esBody1['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
		esBody1['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
		esBody2['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
       		esBody2['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
		esBody3['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
	        esBody3['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
		esBody4['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
       		esBody4['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
		self.levelCounts = es.esSearch(esBody)
		self.proItemCounts = es.esSearch(esBody1)
		self.normalItemCounts = es.esSearch(esBody2)
		self.normalItemNumbers = es.esSearch(esBody4)
		self.userCounts = es.esSearch(esBody3)

	def getnormalItemUseAvg(self):
		normalItemList =[0]*280
		userList = [0]*280
		self.itemUserAvg = [0]*280
		for item in self.userCounts['aggregations']['2']['buckets']:
			userList[item['key']-1] = item['1']['value']
		for item in self.normalItemNumbers['aggregations']['2']['buckets']:
			normalItemList[item['key']-1] = item['1']['value']
		bb = zip(userList,normalItemList)
		i = 0
		for user,items in bb:
			if(0 == user):
				i+=1
			else:
				avg = "%.2f" %(items/user)
				self.itemUserAvg[i] = avg
				i+=1
		return self.itemUserAvg
	def getItemUseRate(self):
		levelList = [0]*280
		proItemList = [0]*280
		normalItemList = [0]*280
		self.proResult = [0]*280
		self.normalResult = [0]*280
		for item in self.levelCounts['aggregations']['2']['buckets']:
			levelList[item['key']-1] = item['doc_count']
		for item in self.proItemCounts['aggregations']['2']['buckets']:
			proItemList[item['key']-1] = item['1']['value']
		for item in self.normalItemCounts['aggregations']['2']['buckets']:
			normalItemList[item['key']-1] = item['1']['value']
		bb = zip(levelList,proItemList,normalItemList)
		i = 0
		for levelCount,proItem,normalItem in bb:
			levelCount  = float(levelCount)
			if (0 == levelCount):
				self.proResult[i] = 0
				self.normalResult[i] = 0
				i+=1
			else:
				proRate = "%.2f" %(proItem/levelCount*100)
				normalRate = "%.2f" %(normalItem/levelCount*100)
				self.proResult[i] = proRate
				self.normalResult[i] = normalRate
				i+=1
	def getProItemUseRate(self):
		return self.proResult
	def getNormalItemUseRate(self):
		return self.normalResult
