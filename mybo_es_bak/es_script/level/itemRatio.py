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

def getItemRatio(es,startTime,endTime):
	print "starting get the item counts ...."
	esBody = {"size":0,"query":{"filtered":{"query":{"query_string":{"query":"LogType:itemsLog","analyze_wildcard":true}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"gte":1469318400000,"lte":1469404800000,"format":"epoch_millis"}}}],"must_not":[]}}}},"aggs":{"2":{"terms":{"field":"level","size":280,"order":{"_term":"asc"}},"aggs":{"3":{"filters":{"filters":{"*":{"query":{"query_string":{"query":"*","analyze_wildcard":true}}},"item_id:1281":{"query":{"query_string":{"query":"item_id:1281","analyze_wildcard":true}}},"item_id:1282":{"query":{"query_string":{"query":"item_id:1282","analyze_wildcard":true}}},"item_id:1283":{"query":{"query_string":{"query":"item_id:1283","analyze_wildcard":true}}},"item_id:1284":{"query":{"query_string":{"query":"item_id:1284","analyze_wildcard":true}}},"item_id:1285":{"query":{"query_string":{"query":"item_id:1285","analyze_wildcard":true}}},"item_id:1286":{"query":{"query_string":{"query":"item_id:1286","analyze_wildcard":true}}},"item_id:1537":{"query":{"query_string":{"query":"item_id:1537","analyze_wildcard":true}}},"item_id:1538":{"query":{"query_string":{"query":"item_id:1538","analyze_wildcard":true}}},"item_id:1539":{"query":{"query_string":{"query":"item_id:1539","analyze_wildcard":true}}},"item_id:1540":{"query":{"query_string":{"query":"item_id:1540","analyze_wildcard":true}}},"item_id:1541":{"query":{"query_string":{"query":"item_id:1541","analyze_wildcard":true}}}}},"aggs":{"1":{"sum":{"field":"number"}}}}}}}}
	sTime = startTime*1000
	eTime = endTime*1000
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
       	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	levelCounts = es.esSearch(esBody)

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
