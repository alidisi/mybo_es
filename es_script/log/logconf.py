#!/usr/bin/env python 
#coding:utf-8

import logging

def log():
	logging.basicConfig(level=logging.ERROR,
			format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
			datefmt='%a, %d %b %Y %H:%M:%S',
			filename = './log/es_query.log',
			filemode='a')
	logging.info('log conf')

log()
	
