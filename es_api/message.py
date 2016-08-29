class Message(object):
    SUCCESS = {'code':1000,'message':'success'}
    ARGUMENT_LOST = {'code':1001,'message':'argument lost'}
    ARGUMENT_ERROR = {'code':1002,'message':'argument error'}
    SYSTEM_ERROR = {'code':1003,'message':'system error'}