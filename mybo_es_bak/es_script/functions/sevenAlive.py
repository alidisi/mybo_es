#!/usr/bin/env python 
#coding:utf-8
from __future__ import division
import es_con

def sevenAlive(es,startTime,endTime,n):
	(proStartTime,proEndTime,ProDay) = es_con.dayTimeStamp(n+7)
	esYesBody = {
		"query": {
			"filtered": {
				"query": {
					"query_string": {
						"analyze_wildcard": True,
						"query": 'LogType:createUserLog'
					}
				},
				"filter": {
					"bool": {
						"must": [{
							"range": {
								"actTime": {
									"gte": 0,
									"lte": 0
								}
							}
						}],
						"must_not": []
					}
				}
			}
		},
		"aggs": {
			"1": {
				"cardinality": {
					"field": 'uuid'
				}
			}
		}
	}
	esProBody = esYesBody
	esYesBody['query']['filtered']['query']['query_string']['query'] =  'LogType:scoreLog AND register_time:[%d TO %d]' %(proStartTime,proEndTime)
	esYesBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
	esYesBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	yesResult = es.esSearch(esYesBody)
#	print "the esYesBody is : %s" %esYesBody
	esProBody['query']['filtered']['query']['query_string']['query']  = "LogType:createUserLog AND createtime:[%d TO %d]" %(proStartTime,proEndTime)
	esProBody['query']['filtered']['filter']['bool']['must'] = []
#	print "the esProBody is : %s" %esProBody
	proResult = es.esSearch(esProBody)
	print "the sevenProDay is %s" %ProDay
	print "the proStartTime is %s" %proStartTime
	print "the proEndTime is %s" %proEndTime
	print "the yesResult is :%d" %yesResult['aggregations']['1']['value']
	print "the proResult is :%d" %proResult['aggregations']['1']['value']
	result =  round(yesResult['aggregations']['1']['value']/proResult['aggregations']['1']['value']*100,5)
	return result
