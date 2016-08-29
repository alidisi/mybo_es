from django.http import HttpResponse
from rest_framework.renderers import JSONRenderer
from es_api.serializers import *
from es_query.models import *
from es_api.message import *
from collections import OrderedDict
import time


class JSONResponse(HttpResponse):
    def __init__(self, data, code=Message.SUCCESS, **kwargs):
        # global responseMsg
        responseMsg = code
        responseMsg['data'] = data
        content = JSONRenderer().render(responseMsg)
        kwargs['content_type'] = 'application/json'
        super(JSONResponse, self).__init__(content, **kwargs)


class ApiList():
    @staticmethod
    def timeSummary(request):
        startTime = request.POST.get('startTime')
        endTime = request.POST.get('endTime')
        if (startTime == None or endTime == None):
            return JSONResponse(None, Message.ARGUMENT_LOST)
        start = time.mktime(time.strptime(startTime, '%Y-%m-%d'))
        end = time.mktime(time.strptime(endTime, '%Y-%m-%d'))
        s = Timesummary.getByTime(start, end)
        ser = TimesummarySerializer(s, many=True)
        return JSONResponse(ser.data)

    @staticmethod
    def levelSummary(request):
        date = request.POST.get('date')
        if date == None:
            return JSONResponse(None, Message.ARGUMENT_LOST)
        dateStamp = time.mktime(time.strptime(date, '%Y-%m-%d'))
        l = Levelsummary.objects.filter(actTime=dateStamp)
        ser = LevelsummarySerializer(l, many=True)
        return JSONResponse(ser.data)

    @staticmethod
    def levelSummaryOne(request):
        level = request.POST.get('level')
        if level == None:
            return JSONResponse(None, Message.ARGUMENT_LOST)
        l = Levelsummary.objects.filter(level=level)
        ser = LevelsummarySerializer(l, many=True)
        return JSONResponse(ser.data)

    @staticmethod
    def exit(request):
        date = request.POST.get('date')
        if date == None:
            return JSONResponse(None, Message.ARGUMENT_LOST)
        date = time.mktime(time.strptime(date, '%Y-%m-%d'))
        l = Exit.objects.filter(actTime=date)
        ser = ExitSerializer(l, many=True)
        return JSONResponse(ser.data)

    @staticmethod
    def exitOne(request):
        level = request.POST.get('level')
        if level == None:
            return  (None, Message.ARGUMENT_LOST)
        l = Exit.objects.filter(level=level)
        ser = ExitSerializer(l, many=True)
        return JSONResponse(ser.data)

    @staticmethod
    def itemUse(request):
        date = request.POST.get('date')
        if date == None:
            return JSONResponse(None, Message.ARGUMENT_LOST)
        date = time.mktime(time.strptime(date, '%Y-%m-%d'))
        l = Itemuse.objects.filter(actTime=date)
        ser = ItemuseSerializer(l, many=True)
        return JSONResponse(ser.data)

    @staticmethod
    def itemUseOne(request):
        level = request.POST.get('level')
        if level == None:
            return JSONResponse(None, Message.ARGUMENT_LOST)
        l = Itemuse.objects.filter(level=level)
        ser = ItemuseSerializer(l, many=True)
        return JSONResponse(ser.data)
    @staticmethod
    def itemDay(request):
	startTime = request.POST.get('startTime')
        endTime = request.POST.get('endTime')
        if (startTime == None or endTime == None):
            return JSONResponse(None, Message.ARGUMENT_LOST)
	start= time.mktime(time.strptime(startTime, '%Y-%m-%d'))
	end= time.mktime(time.strptime(endTime, '%Y-%m-%d'))
        s = Itemday.getByTime(start,end)
        ser = ItemdaySerializer(s, many=True)
	day = set()
	for k in ser.data:
		day.add(k['actTime'])
	tmp =[]
	for d in day:	
		result = {}
		result['actTime'] = d
		for item in ser.data:
			if(item['actTime'] == result['actTime']):
				result['actTime'] = item['actTime']
				item_rate = "%d_rate" %item['items']
				item_num = "%d_num" %item['items']
				result[item_rate] = item['itemRate']
				result[item_num] = item['itemNumbers']
		result = OrderedDict(result)
		tmp.append(result)
        return JSONResponse(tmp)
