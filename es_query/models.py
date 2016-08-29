# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
# Create your models here.
class Timesummary(models.Model):
    actTime = models.IntegerField()                                 #当天0点utc 时间戳
    DAU = models.IntegerField()                                     #当天活跃用户数
    DNU = models.IntegerField()                                     #当天新增用户数
    nextAlive = models.FloatField(blank=True)                       #次日存活率
    sevenAlive = models.FloatField(blank=True)                      #7天存活率
    allInstall = models.FloatField(blank=True)                      #当天为止的所有用户数
    PU = models.IntegerField()                                      #当天付费用户数
    NPU = models.IntegerField()                                     #当天新增付费用户
    APU = models.IntegerField()                                     #当天活跃付费用户
    income = models.FloatField(blank=True)                          #当天收入
    ARPU =  models.FloatField(blank=True)                           #平均用户收入
    ARPPU = models.FloatField(blank=True)                           #平均付费用户收入
    payRate = models.FloatField(blank=True)                         #'付款率PU/DAU',

    @staticmethod
    def getByTime(start, end):
        p = Timesummary.objects.all()
        p = p.filter(actTime__gte=start)
        p = p.filter(actTime__lte=end)
        return p

class Levelsummary(models.Model):
    actTime = models.IntegerField()
    level = models.IntegerField()
    passRate_count = models.FloatField(blank=True)
    passWithItemRate = models.FloatField(blank=True)
    passWithoutItemRate = models.FloatField(blank=True)
    passFullStarRate = models.FloatField(blank=True)
    tryCount = models.FloatField(blank=True)
    failCountAvg =models.FloatField(blank=True)
    firstPassUser = models.IntegerField()
    normalItemUseRate = models.IntegerField()
    proItemUseRate = models.CharField(max_length=255)
    stopUserCount = models.IntegerField()
    stopRate = models.FloatField(blank=True)
    failInStopUser = models.FloatField(blank=True)
    IAPInStopUser = models.FloatField(blank=True)
    loseUser = models.IntegerField()
    loseuserRate = models.FloatField(blank=True)
    stepAvg = models.FloatField(blank=True)
    tryMedian = models.FloatField(blank=True)
    tryQuantile = models.FloatField(blank=True)
    difficty = models.FloatField(blank=True)
    normalItemUseAvg = models.FloatField(blank=True)
    itemIAPAvg = models.FloatField(blank=True)

class Exit(models.Model):
    totalExit = models.IntegerField()
    level = models.IntegerField()
    playCounts = models.IntegerField()
    exitRate = models.FloatField(blank=True)
    manuallyExit = models.IntegerField()
    restart = models.IntegerField()
    actTime = models.IntegerField()

class Itemuse(models.Model):
    level = models.IntegerField()
    actTime = models.IntegerField()
    items = models.IntegerField()
    itemRate = models.FloatField(blank=True)
class Itemday(models.Model):
	actTime = models.IntegerField()
	items = models.IntegerField()
	itemRate = models.FloatField(blank=True)
	itemNumbers = models.IntegerField()
	@staticmethod
	def getByTime(start,end):
		p = Itemday.objects.all()
		p = p.filter(actTime__gte=start)
		p = p.filter(actTime__lte=end)
		return p
