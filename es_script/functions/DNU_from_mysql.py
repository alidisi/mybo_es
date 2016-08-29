#!/usr/bin/env python 
#coding:utf-8

import es_con
import MySQLdb as mysql
import datetime
import time


import mysqlConf

def getDNUCounts(startTime):
#	conn = mysql.connect(host='127.0.0.1',user='mybodev',passwd='mybo-dev',db='test')
	conn = mysqlConf.connMysql()
	cursor = conn.cursor()
	sql = 'SELECT COUNT(*) FROM es_query_alluuid  WHERE actTime =%d' %startTime
	cursor.execute(sql)
	data = cursor.fetchone()
	dnuCounts = data[0]
	return dnuCounts
