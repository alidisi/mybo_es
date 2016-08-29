#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time

class loseAndStop(object):
	def __init__(self,es,es1,startTime,endTime):
		self.es= es
		self.es1 =es1
		self.startTime = startTime
		self.endTime =endTime
		self.loseDenomList =[0]*280
		
	def getLoseDenom(self):
		print "starting get the item counts ...."
		esBody={ "size": 0, "query": { "filtered": { "query": { "query_string": { "query": "LogType:scoreLog AND isFirstPass:1", "analyze_wildcard": True } }, "filter": { "bool": { "must": [ { "range": { "actTime": { "gte": 0, "lte": 0} } } ], "must_not": [] } } } }, "aggs": { "2": { "terms": { "field": "level", "size": 280, "order": { "_term": "asc" } } } } }
		esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =self.startTime
 		esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = self.endTime
		result  = self.es.esSearch(esBody)
		self.loseDenom(result)
	def loseDenom(self,result):
		for item in result['aggregations']['2']['buckets']:
			self.loseDenomList[item['key']-1] = "%.2f" % item['doc_count']
		print self.loseDenomList
	def getTopLevel(self):
		print "starting get top level of the pass user"
		firstPassUserBody  = {"fields":["win","channel","map_version","map_name","level","uuid"], "query": { "filtered": { "query": { "query_string": { "query": "LogType:scoreLog AND isFirstPass:1", "analyze_wildcard": True } }, "filter": { "bool": { "must": [ { "range": { "actTime": { "gte": 0, "lte": 0} } } ], "must_not": [] } } } }} 
		firstPassUserBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =self.startTime
		firstPassUserBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = self.endTime
		result = self.es1.esSearch(firstPassUserBody)
		print result

