#!/usr/env/env python 
#coding:utf-8

from elasticsearch import Elasticsearch
import time 
import datetime
import os 


class esClass(object):
	def __init__(self,esIndex):
		print "esClass object"
		#self.es = Elasticsearch([{'host':'127.0.0.1','port':9200}])
		self.es = Elasticsearch([{'host':'52.221.248.142','port':9110}])
		self.esIndex = esIndex

	def esSearch(self,esBody):
		result = self.es.search(
				index=self.esIndex,
				size = 0,
				body=esBody)
		return result
#es的查询结果中有字段值类
class esResult(object):
	def __init__(self,esIndex):
		print "esResult object"
		#self.es = Elasticsearch([{'host':'192.168.245.129','port':9200}])
		self.es = Elasticsearch([{'host':'52.221.248.142','port':9110}])
		self.esIndex = esIndex

	def esSearch(self,esBody):
		result = self.es.search(
				index=self.esIndex,
				scroll = '5m',
				search_type = 'scan',
				size = 500,
				body = esBody)
		es_list = []
		sid = result['_scroll_id']
		scroll_size = result['hits']['total']
		while(scroll_size > 0):
			print "Scrolling ......"
	#		print sid
	#		try:
			scroResult = self.es.scroll(scroll_id = sid,scroll='5m')
	#		except:
	#			print "conn es faill try conn again ...................."
	#			continue
			eshits = scroResult['hits']['hits']
			for item in eshits:
				es_list.append(item['fields'])
			scroll_size = len(scroResult['hits']['hits'])
			sid = scroResult['_scroll_id']
	#		print "the get sid is %s" %sid
		return es_list
def dayTimeStamp(n):
	        #获取昨天的时间
	YesterDay = datetime.date.today() - datetime.timedelta(days=n)
        #获取昨天时间的开始值
	YesMin = datetime.datetime.combine(YesterDay,datetime.time.min)
        #print type(YesMin)
        #获取昨天时间的最大值
	YesMax = datetime.datetime.combine(YesterDay,datetime.time.max)
        #print type(YesMax)
        #把datetime 对象转化为 str 类型
	YesMin_str = YesMin.strftime("%Y-%m-%d %H:%M:%S")
	YesMax_str = YesMax.strftime("%Y-%m-%d %H:%M:%S")
        #把str 类型的 时间转化为 time.time_struct
	YesMin_str = time.strptime(YesMin_str,"%Y-%m-%d %H:%M:%S")
     #  print YesMin_str
	YesMax_str = time.strptime(YesMax_str,"%Y-%m-%d %H:%M:%S")
  #     print YesMax_str
        #把time.time_struct 格式的时间转化为 timestamp
	YesMin_timeStamp = int(time.mktime(YesMin_str))
	YesMax_timeStamp = int(time.mktime(YesMax_str))
	return(YesMin_timeStamp,YesMax_timeStamp,YesterDay)
