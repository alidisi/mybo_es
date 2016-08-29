"""mybo_es URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import include,url
from django.contrib import admin
from es_api import views as api
#from ../es_api import views
urlpatterns = [
    url(r'^timesummary/$',api.ApiList.timeSummary),
    url(r'^levelsummary/$',api.ApiList.levelSummary),
    url(r'^levelsummaryOne/$',api.ApiList.levelSummaryOne),
    url(r'^exit/$',api.ApiList.exit),
    url(r'^exitOne/$',api.ApiList.exitOne),
    url(r'^itemuse/$',api.ApiList.itemUse),
    url(r'^itemuseOne/$',api.ApiList.itemUseOne),
    url(r'^itemday/$',api.ApiList.itemDay)
]
