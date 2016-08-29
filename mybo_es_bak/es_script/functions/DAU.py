#!/usr/bin/env python 
#coding:utf-8
import es_con

def DAU(es,startTime,endTime):
#	index = "log-business-*"
#	es = es_con.esClass(index)
#	(startTime,endTime) = es.dayTimeStamp(15)
	esBody = {
		"query": {
			"filtered": {
				"query": {
					"query_string": {
						"analyze_wildcard": True,
						"query": 'LogType:scoreLog'
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
					"field": 'customerId'
				}
			}
		}
	}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	print esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte']
	print esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte']
	result = es.esSearch(esBody)
	return result['aggregations']['1']['value']
