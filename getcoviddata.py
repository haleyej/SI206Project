#covid data api, organize by week
# create database that organizes data by week (covid deaths, tweets, and NYT mentions
# -Calculating data (percent change, totals, etc) fromCDC API 
# - nationaldata by week for March 2020-March 2021
# -Get data points into a dictionary by week-Week of the year: deaths from that week
# -Setting up SQL to output all data from all APIs toa database to be usedfor visualization in GGplot
# -Each week is a row in the table
import requests
import os
import sqlite3
import pandas as pd
from sodapy import Socrata


def get_data():
    myapp_token = "1OabKlWEiltQ0w4gC7C7XJd6Q"
    client = Socrata("data.cdc.gov", myapp_token)

    resultsUS1 = client.get("r8kw-7aab", state = "United States",year = 2020, where = "mmwr_week>11", limit=21)  #returns 42 weeks data
    resultsUS2 = client.get("r8kw-7aab", state = "United States", year = 2020, where = "mmwr_week>31") 
    resultsUS3 = client.get("r8kw-7aab", state = "United States",year = 2021, where = "mmwr_week<12")
    
    resultsMI1 = client.get("r8kw-7aab",state = "Michigan",year = 2020, where = "mmwr_week>11", limit=21)
    resultsMI2 = client.get("r8kw-7aab", state = "Michigan", year = 2020, where = "mmwr_week>31")
    resultsMI3=client.get("r8kw-7aab",state = "Michigan",year = 2021, where = "mmwr_week<12")

    midict ={}
    usdict={}

    for result in resultsMI1:
        midict[result['start_date'][:10]]=int(result['covid_19_deaths'])
    for result in resultsMI2:
        midict[result['start_date'][:10]]=int(result['covid_19_deaths'])
    for result in resultsMI3:
        midict[result['start_date'][:10]]=int(result['covid_19_deaths'])
    for result in resultsUS2:
        usdict[result['start_date'][:10]]=int(result["covid_19_deaths"])
    for result in resultsUS1:
        usdict[result['start_date'][:10]]=int(result["covid_19_deaths"])
    for result in resultsUS3:
        usdict[result['start_date'][:10]]=int(result["covid_19_deaths"])
    return [midict, usdict]


"""
database structure: 
- join on week's start date - month-day-year  [2 digits each]
    - primary key

- covid table 
    - week, covid deaths 
- nyt table
    - by week, # of articles mentioning covid-19
- twitter table 
    - by week, 
"""
def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn
def create_db(midata, usdata):
    cur, conn = setUpDatabase('covid_deaths.db')
    cur.execute("CREATE TABLE IF NOT EXISTS covid_deaths (wk TEXT, loc TEXT, deaths NUMBER)")
    cur.execute("CREATE TABLE IF NOT EXISTS weeks (id  INTEGER PRIMARY KEY, week TEXT UNIQUE)")
    for i in range(len(usdata.keys())):
        weeks = list(usdata.keys())
        week = weeks[i]
        deaths = usdata[week]
        loc = 'US'
        
        cur.execute('''INSERT OR IGNORE INTO weeks (week) VALUES (?)''', (week,))
        cur.execute('''SELECT id FROM weeks where week = (?)''', (week,))
        week_id = cur.fetchone()[0]
        cur.execute("INSERT INTO covid_deaths (wk,loc, deaths) VALUES (?,?,?)", (week_id,loc,deaths))
        conn.commit()
    for i in range(len(midata.keys())):
        weeks = list(midata.keys())
        week = weeks[i]
        deaths = midata[week]
        loc = 'MI'
        
        cur.execute('''INSERT OR IGNORE INTO weeks (week) VALUES (?)''', (week,))
        cur.execute('''SELECT id FROM weeks where week = (?)''', (week,))
        week_id = cur.fetchone()[0]
        cur.execute("INSERT INTO covid_deaths (wk,loc,deaths) VALUES (?,?,?)", (week_id,loc,deaths))
        conn.commit()
    # set primary keys ?? how to do this
def main(): 
    midata = get_data()[0]
    usdata = get_data()[1]
    create_db(midata,usdata)
if __name__ =="__main__":
    main()