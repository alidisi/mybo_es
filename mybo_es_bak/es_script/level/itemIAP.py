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
#itemPrice = {
#       ["1281"] = 20/3,
#        ["1282"] = 40/3,
#       ["1283"] = 80/3,
#        ["1284"] = 100/3,
#        ["1285"] = 75/3,
#    }


def getItemIAP_counts(es,startTime,endTime):
	print "starting get the item counts ...."
	esBody={"size":0,"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:itemsLog  AND item_id:(1281 OR 1282 OR 1283 OR 1284 OR 1285)'}},"filter": {"bool": {"must": [{"range": {"@timestamp": {"gte": 0,"lte": 0,"format": "epoch_millis"}}}],"must_not": []}}}},"aggs": {"2": {"terms":{"field": "level","size": 280,"order": {"_term": "asc"}},"aggs":{"1":{"cardinality": {"field": "customer_id"}}}}}}
	esBody1={"size":0,"query":{"filtered":{"query":{"query_string":{"query":"LogType:itemsLog AND item_id:(1281 OR 1282 OR 1283 OR 1284 OR 1285)","analyze_wildcard":True}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"gte":1469232000000,"lte":1469318400000,"format":"epoch_millis"}}}],"must_not":[]}}}},"aggs":{"2":{"terms":{"field":"level","size":280,"order":{"_term":"asc"}},"aggs":{"3":{"filters":{"filters":{"item_id:1281":{"query":{"query_string":{"query":"item_id:1281","analyze_wildcard":True}}},"item_id:1282":{"query":{"query_string":{"query":"item_id:1282","analyze_wildcard":True}}},"item_id:1283":{"query":{"query_string":{"query":"item_id:1283","analyze_wildcard":True}}},"item_id:1284":{"query":{"query_string":{"query":"item_id:1284","analyze_wildcard":True}}},"item_id:1285":{"query":{"query_string":{"query":"item_id:1285","analyze_wildcard":True}}}}},"aggs":{"1":{"sum":{"field":"number"}}}}}}}}
	sTime = startTime*1000
	eTime = endTime*1000
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	esBody1['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['gte'] =sTime
        esBody1['query']['filtered']['filter']['bool']['must'][0]['range']['@timestamp']['lte'] = eTime
	userCounts = es.esSearch(esBody)
	itemCounts = es.esSearch(esBody1)
	itemIAPList = getItemIAP(itemCounts['aggregations']['2']['buckets'])
	itemIAPAvg = getItemIAPAvg(itemIAPList,userCounts['aggregations']['2']['buckets'])
	return itemIAPAvg
def getItemIAP(itemCounts):
	itemIAPList =[0]*280
	for item in itemCounts:
		a = item['3']['buckets']['item_id:1281']['1']['value']
		b = item['3']['buckets']['item_id:1282']['1']['value']
		c = item['3']['buckets']['item_id:1283']['1']['value']
		d = item['3']['buckets']['item_id:1284']['1']['value']
		e = item['3']['buckets']['item_id:1285']['1']['value']
		itemIAPList[item['key']-1] = "%.2f" %(a*20/3+b*40/3+c*80/3+ d*100/3 + e*75/3)
	return itemIAPList
def getItemIAPAvg(itemIAPList,userCounts):
	userCountsList = [0]*280
	itemAvgList = [0]*280
	for item in userCounts:
		userCountsList[item['key']-1] = item['1']['value']
	bb = zip(itemIAPList,userCountsList)
	i=0
	for item,user in bb:
		item=float(item)
		if(0 == user):
			i+=1
		else:
			avg = "%.2f" %(item/user)
			itemAvgList[i] = avg
			i+=1
	return itemAvgList
