#!/usr/bin/env python
#coding:utf-8
from __future__ import division
import logging
from log.logconf import log
import MySQLdb as mysql
from functions import DAU,es_con,DNU,nextAlive,sevenAlive,getNUser,insertNPU,APU,income,DNU_from_mysql,PU
from functions import mysqlConf
from level import firstPassUser
from level import insertTopLevel
from level import passRate_user
from level import passRate_count
from level import passWithItem
from level import passWithoutItem
from level import fullStarRate
from level import failCountsAvg
from level import itemUse
from level import itemIAP
from level import stepAvg
from level import itemUseRate
from level import exitCounts
from functions import dayItem
from level import tryCounts
from level import firstPassUser
from level import lose

def insertsql(startTime,dau,dnu,nextalive,sevenalive,allUserCounts,pu,npu,apu,inc,queryDay):
	conn = mysqlConf.connMysql()
        cursor = conn.cursor()
	arpu = inc/dau
	arppu = inc/pu
	payrate = pu/dau
	payrate = 100*payrate
	exitcode = 0
	selectsql = "SELECT * FROM es_query_timesummary WHERE actTime=%d" %startTime
	sql = "INSERT INTO es_query_timesummary(actTime,DAU,DNU,nextAlive,sevenAlive,allInstall,PU,NPU,APU,income,ARPU,ARPPU,payRate,dayTime) VALUES('%d','%d','%d','%.2f','%.2f','%d','%d','%d','%d','%.2f','%.2f','%.2f','%.3f','%s')" %(startTime,dau,dnu,nextalive,sevenalive,allUserCounts,pu,npu,apu,inc,arpu,arppu,payrate,queryDay)
	exitcode  = cursor.execute(selectsql)
	if (0==exitcode):
		try:
			print sql
			cursor.execute(sql)
		except mysql.Error,e:
                        conn.rollback()
                        commit = False
                        print "rollback",e
	conn.commit()

def insertLevel(item,i,startTime,queryDay):
	conn = mysqlConf.connMysql()
	cursor = conn.cursor()
	if ( float(item[1]) > 0):
		difficuty = "%.2f" %(100/float(item[1]) -1)
	else:
		difficuty = 0
	queryDay = str(queryDay)
	selectsql = "SELECT * FROM es_query_levelsummary WHERE actTime='%d' AND level='%d'" %(startTime,i+1)
	code = cursor.execute(selectsql)
	if (0 == code):
		sql = "INSERT INTO es_query_levelsummary(actTime,level,passRate_user,passRate_count,passWithItemRate,passWithoutItemRate,passFullStarRate,failCountAvg,firstPassUser,normalItemUseAvg,normalItemUseRate,proItemUseRate,itemIAPAvg,stepAvg,difficty,dateTime) VALUES('%d','%d','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" %(startTime,i+1,item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],item[9],item[10],item[11],difficuty,queryDay)
		cursor.execute(sql)
	else:
		print "the sql has insert"
	conn.commit()
def getMessage(es,startTime,endTime):
#	getNUser.uuid(es,startTime,endTime)
	income.income(es,startTime,endTime)
	insertNPU.getPU(es,startTime,endTime)
def getLevelSummary(es,startTime,endTime,queryDay):
	passRate_users = passRate_user.passRate_user(es,startTime,endTime)
	passRate_counts = passRate_count.passRate_counts(es,startTime,endTime)
	passWithItemRate = passWithItem.passWithItem_counts(es,startTime,endTime)
	passWithoutItemRate  = passWithoutItem.passWithoutItem_counts(es,startTime,endTime)
	fullStarRates = fullStarRate.fullStar_counts(es,startTime,endTime)
	failCountsAvgs = failCountsAvg.failAvg_counts(es,startTime,endTime)
#		firstPassUser.getUserLevel(es1,startTime,endTime)
	theFirstPassUser = firstPassUser.getFirstPassUser(startTime,endTime)
	item = itemUse.item(es,startTime,endTime)
	normalItemAvg = item.getnormalItemUseAvg()
	proItemUseRate = item.getProItemUseRate()
	normalItemUseRate = item.getNormalItemUseRate()
	itemIAPAvg = itemIAP.getItemIAP_counts(es,startTime,endTime)
	stepAvgs = stepAvg.getStepAvg(es,startTime,endTime)
	bb =zip(passRate_users,passRate_counts,passWithItemRate,passWithoutItemRate,fullStarRates,failCountsAvgs,theFirstPassUser,normalItemAvg,proItemUseRate,normalItemUseRate,itemIAPAvg,stepAvgs)
	i =0 
	for item in bb:
		insertLevel(item,i,startTime,queryDay)
		i += 1
def getTimeSummary(es,es1,startTime,endTime,queryDay):
		dau  = DAU.DAU(es,startTime,endTime)
                dnu = DNU.DNU(es,startTime,endTime)
                nextalive = nextAlive.nextAlive(es,startTime,endTime,n)
                sevenalive = sevenAlive.sevenAlive(es,startTime,endTime,n)
                allUserCounts = getNUser.getAllInStall(es,startTime,endTime)
                pu = PU.PU(es,startTime,endTime)
                npu = insertNPU.getNPUCounts(startTime)
                apu = APU.apuCounts(es1,startTime,endTime)
                inc = income.getIncome(startTime)
                insertsql(startTime,dau,dnu,nextalive,sevenalive,allUserCounts,pu,npu,apu,inc,queryDay)
def updateNew(startTime,tryCount,firstPassUsers,queryDay):
        conn = mysqlConf.connMysql()
        cursor = conn.cursor()
	bb = zip(tryCount,firstPassUsers)
	i=0
	for t,f in bb:
		i+=1
		updatesql = "update es_query_levelsummary set firstPassUser='%s',tryCount='%s' where actTime='%d' AND level='%d'" %(f,t,startTime,i)
		cursor.execute(updatesql)
	conn.commit()
	conn.close()
		

if __name__ == '__main__':
#	logconf()
#	logging.debug('test')
	index = "log-business-*"
	es1 = es_con.esResult(index)
	es = es_con.esClass(index)
	for i in range(0,1):
		n = 6 -i
		(startTime,endTime,queryDay) = es_con.dayTimeStamp(n)
		print queryDay
		print startTime,endTime
		loseStop = lose.loseAndStop(es,es1,startTime,endTime)	
#		loseStop.getLoseDenom()
		loseStop.getTopLevel()
#		tryCount = tryCounts.getTryCount(es,startTime,endTime)
#		firstPassUsers = firstPassUser.firstPassCount(es,startTime,endTime)
#		updateNew(startTime,tryCount,firstPassUsers,queryDay)
#		dayItem.getDayItems(es,startTime,endTime)
#		itemUseRate.getItemUseRate(es,startTime,endTime)
#		exitCounts.getExitRate(es,startTime,endTime)
#		getMessage(es1,startTime,endTime)
#		getNUser.getAllInStall(es,startTime,endTime)
#		getLevelSummary(es,startTime,endTime,queryDay)
#		getTimeSummary(es,es1,startTime,endTime,queryDay)

