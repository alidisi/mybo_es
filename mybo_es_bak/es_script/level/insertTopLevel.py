#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql
import time




import sys
sys.path.append('../')
from functions import es_con
from functions import mysqlConf


def insertTopLevel(es,startTime,endTime):
	print "starting get the TOP  level...."
	esBody={"fields":["uuid","channel","map_version","map_name","level","win"],"query": {"filtered": {"query": {"query_string": {"analyze_wildcard": True,"query": 'LogType:scoreLog'}},"filter": {"bool": {"must": [{"range": {"actTime": {"gte": 0,"lte": 0}}}],"must_not": []}}}}}
	esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['gte'] =startTime
        esBody['query']['filtered']['filter']['bool']['must'][0]['range']['actTime']['lte'] = endTime
	result = es.esSearch(esBody)
	linkMysql(result,startTime)

def linkMysql(result,startTime):
        conn = mysqlConf.connMysql()
        cursor = conn.cursor()
        print "insert TOP level  to TOPLevel....."
	dateTime = time.localtime(startTime)
	dateTime = time.strftime("%Y-%m-%d",dateTime)
        for item in result:
		TOPLevel = getTopLevel(item['uuid'][0],item['level'][0],cursor)
		code = createOrUpdate(item['uuid'][0],startTime,cursor)
                insertSql = "INSERT INTO es_query_topLevel(actTime,uuid,channel,mapName,mapVersion,topLevel,win,dateTime) VALUES('%d','%s','%s','%s','%d','%d','%d','%s')" %(startTime,item['uuid'][0],item['channel'][0],item['map_name'][0],item['map_version'][0],TOPLevel,item['win'][0],dateTime)
                updateSql1 = "UPDATE es_query_topLevel SET topLevel='%d',win='0'  where uuid='%s' AND actTime='%d' " %(TOPLevel,item['uuid'][0],startTime)
                updateSql2 = "UPDATE es_query_topLevel SET topLevel='%d',win='1' where uuid='%s' AND actTime='%d'" %(TOPLevel,item['uuid'][0],startTime)
		try:
			if (0 == code):
			#	print insertSql
				cursor.execute(insertSql)
			else:
				if (1 == item['win'][0] or 5 == item['win'][0]):
			#		print updateSql2
					cursor.execute(updateSql2)
				else:
			#		print updateSql2
					cursor.execute(updateSql1)
		except mysql.Error,e:
			conn.rollback()
			print "rollback",e
	conn.commit()
        conn.close()
def getTopLevel(uuid,level,cursor):
#	print "get TOP Level"
	sql= "SELECT topLevel FROM es_query_topLevel where uuid='%s' ORDER BY toplevel DESC LIMIT 1" %uuid
	code = cursor.execute(sql)
	if (0 == code):
		topLevel = level
	else:
		data = cursor.fetchall()
		print data
		if (data[0][0] >= level):
			topLevel = data[0][0]
		else:
			topLevel = level
	return topLevel
def createOrUpdate(uuid,startTime,cursor):
#	print "update or create"
	sql = "SELECT * FROM es_query_topLevel WHERE uuid='%s' AND actTime='%d' " %(uuid,startTime)
	code = cursor.execute(sql)
	if (0 == code):
		return 0
	else:
		return 1
class stopAndLose(object):
	def __init__(self,startTime):
		self.conn = mysqlConf.connMysql()
		self.cursor = self.conn.cursor()
		self.startTime = startTime
	#根据传入的时间从es_query_topLevel 中拉去所有DAU 的uuid
	def getDataSet(self,sqltime):
		sql = "SELECT uuid FROM es_query_topLevel WHERE actTime='%d'" %sqltime
		self.cursor.execute(sql)
		data = self.cursor.fetchall()
		dataSet = set([])
		for item in data:
			dataSet.add(item[0])
		self.conn.commit()
		return dataSet
	#getDenom 得到第二天toplevel 大于第一天的用户uuid denomSet和关卡分布denom
	def addDenom(self,level1,level2,denom):
                for i in range(level1[0],level2[0]):
			denom[i] += 1
 
	def getDenom(self,Set):
                sqlTime1 = self.startTime -4*86400
                sqlTime2 = self.startTime - 3*86400
                data = [0]*280
                dataSet = set()
                for item in Set:
                        #这里可以通过 sql1拿到的topLevel 到sql2中判断sql2中的topLevel 是否大于sql1中的topLevel
                        sql1 = "SELECT topLevel  FROM es_query_topLevel WHERE actTime='%d' AND uuid='%s'" %(sqlTime1,item)
                        sql2 = "SELECT topLevel  FROM es_query_topLevel WHERE actTime='%d' AND uuid='%s'" %(sqlTime2,item)
                        self.cursor.execute(sql1)
                        level1 = self.cursor.fetchone()
                        self.cursor.execute(sql2)
                        level2 = self.cursor.fetchone()
                        if (level2[0]  > level1[0]):
                                self.addDenom(level1,level2,data)
                                dataSet.add(item)
		return (data,dataSet)
	def getLoseMolecule(self):
		self.loseMolecule = [0]*280
		self.loseMoleculeSet = set()
		set1 = self.loseUserDenomSet -(self.setDict[0] & self.loseUserDenomSet)
		set2 = set1 - (self.setDict[1] & set1)
		set3 = set2 - (self.setDict[2] & set2)
		i = 0
		for item in set3:
			sql = "SELECT topLevel  FROM es_query_topLevel WHERE actTime='%d' AND uuid='%s'" %(self.startTime -3*86400,item)
			self.cursor.execute(sql)
			loseLevel = self.cursor.fetchall()
			self.loseMolecule[loseLevel[0][0]-1]+=1
			self.loseMoleculeSet.add(item)
	def getStopMolecule(self):
                secondDay = self.startTime - 86400*3
                self.stopMolecule = self.loseMolecule[:]
		self.stopUserSet = set()
                for item in self.stopSet:
                        sql1 = "SELECT topLevel FROM es_query_topLevel WHERE actTime='%d' AND uuid='%s' " %(secondDay,item)
                        self.cursor.execute(sql1)
                        data = self.cursor.fetchall()
                        stopLevel = data[0][0]
                        sql2 = "SELECT topLevel FROM es_query_topLevel WHERE actTime>='%d' AND actTime<='%d' AND uuid='%s' AND topLevel>'%d'" %(self.startTime-86400*2,self.startTime,item,stopLevel)
                        code = self.cursor.execute(sql2)
                        if (0 == code):
                                self.stopMolecule[stopLevel-1] += 1
				self.stopUserSet.add(item)
		test = 0
		test1 = 0
		for i in self.stopMolecule:
			test +=i
		for k in self.loseMolecule:
			test1+=k
	#同时返回流失用户数的  list 和用户流失率的list
	def getLoseAndStopStart(self):
		self.setDict = {}
		for i in range(5):
			sqlTime = self.startTime -i*86400
			self.setDict[i] = self.getDataSet(sqlTime)
		#第一天和第二天都活跃的用户的一个集合 allSet
		self.aliveSet = self.setDict[3] & self.setDict[4]
		 #allSet 中过滤出在第二天中topLevel大于第一天 的分母的分布情况self.denom self.loseUserDenomSet是第二天toplevel 大于第一天的uuid的集合
		(self.denom,self.loseUserDenomSet) = self.getDenom(self.aliveSet)
		#在loseSet 中选出在第三，四，五，天都没有活跃的用户，返回各个关卡的分布情况
		self.getLoseMolecule()
		#在allSet中去掉在第三，四，五关都没有活跃的用户，减少计算的次数，
		self.stopSet = self.loseUserDenomSet  - self.loseMoleculeSet
		self.getStopMolecule()
	def getLoserUserCounts(self):
		return self.loseMolecule
	def getStopUserCounts(self):
		return self.stopMolecule
	def getLoserUserRate(self):
		loseUserRate = [0]*280
		print self.denom
		bb = zip(self.denom,self.loseMolecule)
		print bb
		i = 0
		for denom,lose in bb:
			denom = float(denom)
			lose = float(lose)
			if(0 == denom):
				i+=1
			else:
				loseRate = "%.2f" %(lose/denom*100)
				loseUserRate[i] = loseRate
				i+=1
		return loseUserRate
        def getStopUserRate(self):
                stopUserRate = [0]*280
                bb = zip(self.denom,self.stopMolecule)
                i = 0 
                for denom,stop in bb: 
                        denom = float(denom)
                        stop = float(stop)
                        if(0 == denom):
                                i+=1
                        else:
                                stopRate = "%.2f" %(stop/denom*100)
                                stopUserRate[i] = stopRate
				i+=1
                return stopUserRate
	def IAPInStop(self):
		sql = "SELECT uuid FROM es_query_allPU WHERE actTime <='%d'"%(self.startTime)
		self.cursor.execute(sql)
		PUData = self.cursor.fetchall()
		PUSet = set()
		self.allSotpMoleculeSet = (self.loseMoleculeSet  | self.stopUserSet)
		for pu in PUData:
			PUSet.add(pu[0])	
		print len(PUSet)
		print self.allSotpMoleculeSet
		stopPU = self.allSotpMoleculeSet & PUSet
		print stopPU
	def failInStop(self):
		self.failInStop = [0]*280
		self.failInStopRate = [0]*280
		for item in self.loseMoleculeSet:
			sql = "SELECT topLevel FROM es_query_topLevel WHERE uuid='%s' AND actTime='%d' AND win='1'" %(item,self.startTime-3*86400)
			code = self.cursor.execute(sql)
			if(0 == code):
				sql1 = "SELECT topLevel FROM es_query_topLevel WHERE uuid='%s' AND actTime='%d'" %(item,self.startTime-3*86400)
				self.cursor.execute(sql1)
				level = self.cursor.fetchone()
				self.failInStop[level[0]-1]+=1
		for item1 in self.stopUserSet:
			sql = "SELECT topLevel FROM es_query_topLevel WHERE uuid='%s' AND actTime>='%d' AND actTime<='%d'  AND win='1'" %(item1,self.startTime-3*86400,self.startTime)
			code1 = self.cursor.execute(sql)
			if(0 == code1):
				sql1 = "SELECT topLevel FROM es_query_topLevel WHERE uuid='%s' AND actTime='%d'" %(item1,self.startTime -3*86400)	
				self.cursor.execute(sql1)
				level1 = self.cursor.fetchone()
				self.failInStop[level1[0]-1] +=1
			bb = zip(self.failInStop,self.stopMolecule)
			i = 0
			for fail,stop in bb:
				fail = float(fail)
				stop = float(stop)
				if(0 == stop):
					i+=1
				else:
					failRate = "%.2f" %(fail/stop*100)
					self.failInStopRate[i]=failRate
					i+=1		
	#	print self.stopMolecule
	#	print self.failInStop
		return self.failInStopRate
