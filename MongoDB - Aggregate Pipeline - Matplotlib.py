#!/usr/bin/env python
# coding: utf-8


from pymongo import MongoClient
import requests
import pandas as pd
import matplotlib.pylab as plt
import datetime
import pytz
from bson.json_util import dumps



def convertint(bdic):
    for key, value in bdic.items():
        try:
            bdic[key] = float(value)
        except:
            pass
    return bdic

def convertdate(mydstring):
    my_date = datetime.datetime.strptime(mydstring, "%Y-%m-%d")
    return my_date
    



base_url = "https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY&symbol=AAPL&apikey=__API_KEY_Removed___.json"
___
request2 = requests.get(base_url)

reqjsona = request2.json()

wts = reqjsona["Weekly Time Series"]

wts = {outer_key: {inner_key.replace(". ", "_"): inner_val for inner_key, inner_val in outer_val.items()} for outer_key, outer_val in wts.items()}


# establish connecttion parameters
client = MongoClient('127.0.0.1', 27017)
db_name = 'alpha'
weeklytable = "weeklycol"



# connect to the database
db = client[db_name]
weekcol = db[weeklytable]



# main insertation script
for t in wts:
    bdic = {}
    bdic.update(wts[t])
    convertint(bdic)
    
    bdic["date"] = convertdate(t)
    curr_doc_date = bdic["date"] 
    resultab = weekcol.find({"date": curr_doc_date})
    results = list(resultab)
    if len(results) == 0:
        weekcol.insert_one(bdic)


#average of all values example

# open price average. 
openavg = weekcol.aggregate([
    ## stage 1
    {"$match":{}},
    
    
    ## stage 2
     {"$group":{"_id": "", "opening_avg": {"$avg" : "$1_open"}}}
    
])

for opena in openavg: 
    print(opena)
###############################################################################    
# close price average. 
closeavg = weekcol.aggregate([
    ## stage 1
    {"$match":{}} ,
    
    
    ## stage 2
     {"$group":{"_id": "", "closing_avg": {"$avg" : "$4_close"}}}
    
])

for closea in closeavg:
    print(closea)

    
###############################################################################  

# low price average. 
lowavg = weekcol.aggregate([
    ## stage 1
    {"$match":{}},
    
    
    ## stage 2
     {"$group":{"_id": "", "low_avg": {"$avg" : "$3_low"}}}
    
])

for lowa in lowavg:
    print(lowa)


############################################################################### 

# Closing price average. 
highavg = weekcol.aggregate([
    ## stage 1
    {"$match":{}},
    
    
    ## stage 2
     {"$group":{"_id": "", "high_avg": {"$avg" : "$2_high"}}}
    
])

for higha in highavg:
    print(higha)

aggpricing = {}

aggpricing.update(opena)
aggpricing.update(closea)
aggpricing.update(lowa)
aggpricing.update(higha)


del aggpricing["_id"]
aggpricing


keys = aggpricing.keys()
values = aggpricing.values()
plt.bar(keys, values)
plt.title("All time Averages")


#works fine: find gte - lte date range - BUT Not through aggregation pipeline

resultwww = weekcol.find({
    "$and" : [{
                 "date" : { "$gt" :datetime.datetime(2020, 12, 30, 0, 0, tzinfo=pytz.utc)}
              },
              {
                   "date" : { "$lt" :datetime.datetime(2021, 1, 30, 0, 0, tzinfo=pytz.utc)}
              }]
})


for ttt in resultwww:
    print(ttt)

# GT and LT date - working fine!!!!!!!!!!!!!!!
betdate = weekcol.aggregate([
    ## stage 1
    {"$match":{
  
"date": {"$gt": datetime.datetime(2019, 12, 31, 0, 0, tzinfo=pytz.utc)}

}}
    ,
    
{"$match":{
  
"date": {"$lt": datetime.datetime(2021, 1, 1, 0, 0, tzinfo=pytz.utc)}

}}
    
])

for bdate in betdate:
    print(bdate)
    


# GT and LT date - working fine!!!!!!!!!!!!!!!
betdate = weekcol.aggregate([
    ## stage 1
    {"$match":{
  
"date": {"$gt": datetime.datetime(2020, 11, 30, 0, 0, tzinfo=pytz.utc)}

}}
    ,
    
{"$match":{
  
"date": {"$lt": datetime.datetime(2020, 12, 31, 0, 0, tzinfo=pytz.utc)}

}},
    ## stage 2
    {
        "$group" : { "_id" : "jan",
                     "average_opening_price" : {
                         "$avg" : "$1_open"
                     },
                    "average_closing_price" : {
                        "$avg" : "$4_close"
                    }
        } 
    }
    
])

jandic = {}
for bdate in betdate:
   
    jandic["Title"] = bdate["_id"]
    jandic["Average_opening_price"] = bdate["average_opening_price"]
    jandic["Average_closing_price"] = bdate["average_closing_price"]
    


del jandic["Title"]


keys = jandic.keys()
values = jandic.values()
plt.bar(keys, values)
plt.title("January")
