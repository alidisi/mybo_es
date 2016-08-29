#!/usr/bin/env python 
#coding:utf-8

import MySQLdb as mysql

def connMysql():
	conn = mysql.connect(host='127.0.0.1',user='mybodev',passwd='mybo-dev',db='test')
	return conn
