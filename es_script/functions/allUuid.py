#!/usr/bin/env python 
#coding:utf-8
import MySQLdb as mysql
import datetime
import time


def uuid(es,startTime,endTime):
	esBody = {"fields":["uuid","channel"],"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:createUserLog'}},"filter": {"bool": {"must": [{"range": {"createtime": {"gte": 0,"lte": 0}}}],"must_not": []}}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['createtime']['gte'] =startTime
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['createtime']['lte'] = endTime
#	print esBody
	result = es.esSearch(esBody)
#	print result
	linkMysql(result,startTime)
	success = 1
	return success
	
def linkMysql(result,startTime):
	conn = mysql.connect(host='127.0.0.1',user='mybodev',passwd='mybo-dev',db='test')
	cursor = conn.cursor()
#	actTime = datetime.datetime.utcfromtimestamp(startTime)
#	actTime = actTime.strftime("%Y-%m-%d %H:%M:%S")
	dateTime = time.localtime(startTime)
	dateTime = time.strftime("%Y-%m-%d %H:%M:%S",dateTime)
	uuid_list = []
	for item in result:
		uuid_list.append(item['uuid'][0])
		commit = True
		selectsql = "SELECT * FROM es_query_alluuid WHERE uuid='%s'" %item['uuid'][0]
		sql = "INSERT INTO es_query_alluuid(actTime,uuid,channel,dateTime) VALUES('%d','%s','%s','%s')" %(startTime,item['uuid'][0],item['channel'][0],dateTime)
		try:
			print sql
			uuid_exit = cursor.execute(selectsql)
			if (0 == uuid_exit):
				cursor.execute(sql)
		except mysql.Error,e:
			conn.rollback()
			commit = False
		#	print "rollback",e
		if(commit):
			conn.commit()
	conn.close()
#判断一个列表中每个值得重复次数
def get_counts(sequence):
	counts = {}
	for x in sequence:
		if x in counts:
			counts[x] +=1
		else:
			counts[x] = 1
	return counts
	print "sql insert successful"
def getUserCounts(startTime):
        conn = mysql.connect(host='127.0.0.1',user='mybodev',passwd='mybo-dev',db='test')
        cursor = conn.cursor()
	sql = "SELECT COUNT(*) FROM es_query_alluuid WHERE actTime<= %d" %startTime
	try:
		cursor.execute(sql)
		counts = cursor.fetchone()
	except:
		print "sql error %s" %sql
	return counts[0]

	
