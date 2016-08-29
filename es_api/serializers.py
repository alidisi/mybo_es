from es_query.models import *
from rest_framework import serializers

class LevelSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=100)
    title = serializers.CharField(max_length=100)
    author = serializers.CharField(max_length=100)
    def restore_object(self,attrs,instance):
        if instance:
            instance.title = attrs['title']
            instance.name = attrs['name']
            instance.author = attrs['author']
            return instance
        return Level(**attrs)

class TimesummarySerializer(serializers.Serializer):
    actTime = serializers.IntegerField()
    DAU = serializers.IntegerField()
    DNU = serializers.IntegerField()
    nextAlive = serializers.FloatField()
    sevenAlive = serializers.FloatField()
    allInstall = serializers.FloatField()
    PU = serializers.IntegerField()
    NPU = serializers.IntegerField()
    APU = serializers.IntegerField()
    income = serializers.FloatField()
    ARPU = serializers.FloatField()
    ARPPU = serializers.FloatField()
    payRate = serializers.FloatField()
    def restore_object(self,attrs,instance):
        if instance:
            instance.actTime = attrs['actTime']
            instance.DAU = attrs['DAU']
            instance.DNU = attrs['DNU']
            instance.nextAlive = attrs['nextAlive']
            instance.sevenAlive = attrs['sevenAlive']
            instance.allInstall = attrs['allInstall']
            instance.PU = attrs['PU']
            instance.NPU = attrs['NPU']
            instance.APU = attrs['APU']
            instance.income = attrs['income']
            instance.ARPU = attrs['ARPU']
            instance.ARPPU = attrs['ARPPU']
            instance.payRate = attrs['payRate']
            return instance
        return Timesummary(**attrs)

class LevelsummarySerializer(serializers.Serializer):
    actTime = serializers.IntegerField()
    level = serializers.IntegerField()
    passRate_count = serializers.FloatField()
    passWithItemRate = serializers.FloatField()
    passWithoutItemRate = serializers.FloatField()
    passFullStarRate = serializers.FloatField()
    tryCount = serializers.FloatField()
    failCountAvg = serializers.FloatField()
    firstPassUser = serializers.IntegerField()
    normalItemUseRate = serializers.IntegerField()
    proItemUseRate = serializers.CharField()
    stopUserCount = serializers.IntegerField()
    stopRate = serializers.FloatField()
    failInStopUser = serializers.FloatField()
    IAPInStopUser = serializers.FloatField()
    loseUser = serializers.IntegerField()
    loseuserRate = serializers.FloatField()
    stepAvg = serializers.FloatField()
    tryMedian = serializers.FloatField()
    tryQuantile = serializers.FloatField()
    difficty = serializers.FloatField()
    normalItemUseAvg = serializers.FloatField()
    itemIAPAvg = serializers.FloatField()
    def restore_object(self,attrs,instance):
        if instance:
            instance.actTime = attrs['actTime']
            instance.level = attrs['level']
            instance.passRate_count = attrs['passRate_count']
            instance.passWithItemRate = attrs['passWithItemRate']
            instance.passWithoutItemRate = attrs['passWithoutItemRate']
            instance.passFullStarRate = attrs['passFullStarRate']
            instance.tryCount = attrs['tryCount']
            instance.failCountAvg = attrs['failCountAvg']
            instance.firstPassUser = attrs['firstPassUser']
            instance.normalItemUseRate = attrs['normalItemUseRate']
            instance.proItemUseRate = attrs['proItemUseRate']
            instance.stopUserCount = attrs['stopUserCount']
            instance.stopRate = attrs['stopRate']
            instance.failInStopUser = attrs['failInStopUser']
            instance.IAPInStopUser = attrs['IAPInStopUser']
            instance.loseUser = attrs['loseUser']
            instance.loseuserRate = attrs['loseuserRate']
            instance.stepAvg = attrs['stepAvg']
            instance.tryMedian = attrs['tryMedian']
            instance.tryQuantile = attrs['tryQuantile']
            instance.difficty = attrs['difficty']
            instance.normalItemUseAvg = attrs['normalItemUseAvg']
            instance.itemIAPAvg = attrs['itemIAPAvg']
            return instance
        return Levelsummary(**attrs)

class ExitSerializer(serializers.Serializer):
    totalExit = serializers.IntegerField()
    level = serializers.IntegerField()
    playCounts = serializers.IntegerField()
    exitRate = serializers.FloatField()
    manuallyExit = serializers.IntegerField()
    restart = serializers.IntegerField()
    actTime = serializers.IntegerField()
    def restore_object(self, attrs, instance):
        if instance:
            instance.totalExit = attrs['totalExit']
            instance.level = attrs['level']
            instance.playCounts = attrs['playCounts']
            instance.exitRate = attrs['exitRate']
            instance.manuallyExit = attrs['manuallyExit']
            instance.restart = attrs['restart']
            instance.actTime = attrs['actTime']
            return instance
        return Timesummary(**attrs)
class ItemuseSerializer(serializers.Serializer):
    level = serializers.IntegerField()
    actTime = serializers.IntegerField()
    items = serializers.IntegerField()
    itemRate = serializers.FloatField()
    def restore_object(self, attrs, instance):
        if instance:
            instance.level = attrs['level']
            instance.actTime = attrs['actTime']
            instance.items = attrs['items']
            instance.itemRate = attrs['itemRate']
            return instance
        return Timesummary(**attrs)
class ItemdaySerializer(serializers.Serializer):
	actTime = serializers.IntegerField()
	items = serializers.IntegerField()
	itemRate = serializers.FloatField()
	itemNumbers = serializers.IntegerField()
	def restore_object(self,attrs,instance):
		if instance:
			instance.actTime = attrs['actTime']
			instance.items = attrs['items']
			instance.itemRate = attrs['itemRate']
			instance.itemNumbers = attrs['itemNumbers']
			return instance
		return Itemday(**attrs)
