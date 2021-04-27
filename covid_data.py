import requests
import json
import os
import sqlite3
import pandas as pd
from sodapy import Socrata
import datetime
from datetime import date, timedelta
from bs4 import BeautifulSoup
import re
import csv
import plotly.express as px


##PART 1: TRANSPORT DATA
client = Socrata("data.bts.gov", None)

def get_resp(level, date, state_code = 'MI'):
    '''Takes in three parameters and returns a list of dictionaries with data on trips by distance from the Bureau 
        of Transportation Statistics. 
        Values for level: National, State (note cases)
        Value for date: Any ISO formated Floating Datetime Object
        Value for state_code: Any two-digit state code (ie CA, AK). MI is the default value'''
    if level == 'State':
        resp = client.get("w96p-f2qv", limit = 25, state_code = state_code, date = date)
    else:
        url = f"https://data.bts.gov/resource/w96p-f2qv.json?level={level}*date={date}&limit=25"
        resp = client.get("w96p-f2qv", level = level, limit=25, date = date)
    return resp


def increment_time(start_date):
    '''Takes in a DateTime object. Increments it by 7 days to jump ahead to the next week. 
    Returns the incremented DateTime object'''
    next_week = start_date + timedelta(days=7)
    return next_week

def build_national_lst(start_date):
    '''Takes in a start date. Calls the get_resp function 53 times to get data for a complete year.
    Appends results from each call to get_resp to a list and returns that list.'''
    national_lst = []
    date = start_date
    for i in range(53):
        data = get_resp(level = 'National', date = date.isoformat())
        national_lst.append(data)
        date = increment_time(date)
    return national_lst

def build_state_lst(start_date, state_code = 'MI'):
    '''Takes in a start date and state code (Michigan is the defaul value). Calls the get_resp function 53 times to get 
    a data dictionary for a complete year. Appends results from each call to get_resp to a list.
    Returns a list of lists of dictionaries.'''
    state_lst = []
    date = start_date
    for i in range(53):
        data = get_resp(level = 'State', date = date.isoformat(), state_code = state_code)
        state_lst.append(data)
        date = increment_time(date)
    return state_lst

def setUpDatabase(db_name):
    '''Takes in the name of a database. Creates the database and returns a cur and conn'''
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn


def create_transport_database(national_lst, state_lst):
    '''Takes in a list of dictionaries of state data and list of dictionaries of national data. 
    Extracts values from the list, conver them from strings to floating points, inserts values 
    into a table and commits the changes. Does not return anything'''
    cur, conn = setUpDatabase('full_data.db')
    cur.execute('''CREATE TABLE IF NOT EXISTS transport (i INTEGER PRIMARY KEY UNIQUE, week_id INTEGER, location_id INTEGER, at_home INTEGER,
                not_at_home INTEGER, ratio_at_home INTEGER, one_mile_trips INTEGER, one_to_three_mile_trips INTEGER,
                three_to_five_mile_trips INTGER, short_trips INTEGER, hundred_to_250_mile_trips INTEGER,
                twohundredfifty_to_500_mile_trips INTEGER, long_trips INTEGER, ratio_short_to_long INTEGER)''')
    cur.execute("CREATE TABLE IF NOT EXISTS weeks (id INTEGER PRIMARY KEY, week TEXT UNIQUE)")
    cur.execute("CREATE TABLE IF NOT EXISTS locs (id  INTEGER PRIMARY KEY, loc TEXT UNIQUE)")
    for i in range(len(national_lst)):
        week = str(national_lst[i][0]['date'])[:10]
        cur.execute('''INSERT OR IGNORE INTO weeks (week) VALUES (?)''', (week,))
        cur.execute('''SELECT id FROM weeks where week = (?)''', (week,))
        week_id = cur.fetchone()[0]
        location = national_lst[i][0]['level']
        cur.execute('''INSERT OR IGNORE INTO locs (loc) VALUES (?)''', (location,))
        cur.execute('''SELECT id FROM locs where loc = (?)''', (location,))
        location_id = cur.fetchone()[0]
        at_home = float(national_lst[i][0]['pop_stay_at_home'])
        not_at_home = float(national_lst[i][0]['pop_not_stay_at_home'])
        ratio_at_home = (at_home) / (not_at_home)
        one_mile_trips = float(national_lst[i][0]['trips_1'])
        one_to_three_mile_trips = float(national_lst[i][0]['trips_1_3'])
        three_to_five_mile_trips = float(national_lst[i][0]['trips_3_5'])
        short_trips = float(national_lst[i][0]['trips_10_25']) + float(national_lst[i][0]['trips_25_50'])
        hundred_to_250_mile_trips = float(national_lst[i][0]['trips_100_250'])
        twohundredfifty_to_500_mile_trips = float(national_lst[i][0]['trips_250_500'])
        long_trips = float(national_lst[i][0]['trips_500'])
        try:
            ratio_short_to_long= short_trips / long_trips
        except:
            ratio_short_to_long = 0
        cur.execute('''INSERT OR IGNORE INTO transport (i, week_id, location_id, at_home, not_at_home, ratio_at_home,
                    one_mile_trips, one_to_three_mile_trips, three_to_five_mile_trips, short_trips, hundred_to_250_mile_trips,
                    twohundredfifty_to_500_mile_trips, long_trips, ratio_short_to_long) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                    (i, week_id, location_id, at_home, not_at_home, ratio_at_home, one_mile_trips, one_to_three_mile_trips, 
                    three_to_five_mile_trips, short_trips, hundred_to_250_mile_trips, twohundredfifty_to_500_mile_trips, 
                    long_trips, ratio_short_to_long))
    conn.commit()
    primary_key = len(state_lst)
    for i in range(len(state_lst)):
        primary_key += 1
        week = str(state_lst[i][0]['date'])[:10]
        cur.execute('''INSERT OR IGNORE INTO weeks (week) VALUES (?)''', (week,))
        cur.execute('''SELECT id FROM weeks where week = (?)''', (week,))
        week_id = cur.fetchone()[0]
        location = state_lst[i][0]['state_code'] 
        cur.execute('''INSERT OR IGNORE INTO locs (loc) VALUES (?)''', (location,))
        cur.execute('''SELECT id FROM locs where loc = (?)''', (location,))
        location_id = cur.fetchone()[0]
        at_home = float(state_lst[i][0]['pop_stay_at_home'])
        not_at_home = float(state_lst[i][0]['pop_not_stay_at_home'])
        ratio_at_home = (at_home) / (not_at_home)
        one_mile_trips = float(state_lst[i][0]['trips_1'])
        one_to_three_mile_trips = float(state_lst[i][0]['trips_1_3'])
        three_to_five_mile_trips = float(state_lst[i][0]['trips_3_5'])
        short_trips = float(state_lst[i][0]['trips_10_25']) + float(state_lst[i][0]['trips_25_50'])
        hundred_to_250_mile_trips = float(state_lst[i][0]['trips_100_250'])
        twohundredfifty_to_500_mile_trips = float(state_lst[i][0]['trips_250_500'])
        long_trips = float(state_lst[i][0]['trips_500'])
        try:
            ratio_short_to_long= short_trips / long_trips
        except:
            ratio_short_to_long = 0
        cur.execute('''INSERT OR IGNORE INTO transport (i, week_id, location_id, at_home, not_at_home, ratio_at_home,
                    one_mile_trips, one_to_three_mile_trips, three_to_five_mile_trips, short_trips, hundred_to_250_mile_trips,
                    twohundredfifty_to_500_mile_trips, long_trips, ratio_short_to_long) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                    (primary_key, week_id, location_id, at_home, not_at_home, ratio_at_home, one_mile_trips, one_to_three_mile_trips, 
                    three_to_five_mile_trips, short_trips, hundred_to_250_mile_trips, twohundredfifty_to_500_mile_trips, 
                    long_trips, ratio_short_to_long))
    conn.commit()


##PART 2: COVID 19 DATA
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


def create_db(midata, usdata):
    cur, conn = setUpDatabase('full_data.db')
    cur.execute("CREATE TABLE IF NOT EXISTS covid_deaths (i INTEGER PRIMARY KEY, wk TEXT, loc INTEGER, deaths NUMBER)")
    #cur.execute("CREATE TABLE IF NOT EXISTS weeks (id  INTEGER PRIMARY KEY, week TEXT UNIQUE)")
    #cur.execute("CREATE TABLE IF NOT EXISTS locs (id INTEGER, loc TEXT UNIQUE)")
    for i in range(len(usdata.keys())):
        weeks = list(usdata.keys())
        week = weeks[i]
        deaths = usdata[week]
        loc = 'National'
        cur.execute('''INSERT OR IGNORE INTO locs (loc) VALUES (?)''', (loc,))
        cur.execute('''SELECT id FROM locs where loc = (?)''', (loc,))
        loc_id = cur.fetchone()[0]
        cur.execute('''INSERT OR IGNORE INTO weeks (week) VALUES (?)''', (week,))
        cur.execute('''SELECT id FROM weeks where week = (?)''', (week,))
        week_id = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO covid_deaths (i, wk,loc, deaths) VALUES (?,?,?,?)", (i, week_id,loc_id,deaths))
        conn.commit()
    primary_key = len(usdata.keys())
    for i in range(len(midata.keys())):
        primary_key += 1
        weeks = list(midata.keys())
        week = weeks[i]
        deaths = midata[week]
        loc = 'MI'
        cur.execute('''INSERT OR IGNORE INTO locs (loc) VALUES (?)''', (loc,))
        cur.execute('''SELECT id FROM locs where loc = (?)''', (loc,))
        loc_id = cur.fetchone()[0]
        cur.execute('''INSERT OR IGNORE INTO weeks (week) VALUES (?)''', (week,))
        cur.execute('''SELECT id FROM weeks where week = (?)''', (week,))
        week_id = cur.fetchone()[0]
        cur.execute("INSERT OR IGNORE INTO covid_deaths (i, wk,loc,deaths) VALUES (?,?,?,?)", (primary_key, week_id,loc_id,deaths))
        conn.commit()
    # set primary keys ?? how to do this



#PART 3: UNEMPLOYMENT DATA
with open("unemployment_report.html") as f:
        soup = BeautifulSoup(f, "html.parser")


def get_week_dates():
    #Gets start dates of weeks and formats into keys that match the rest of the code
    #Function will return a list of these weekly start dates 
    #List returns dates chronologically from March 2020 to may 2020
    date_list = []

    anchor = soup.find("table")
    rows = anchor.find_all('tr')

    #rows is sliced to get data from specified dates our group decided upon
    for row in rows[13:38]:

        header = row.find('td')['headers']
        date = header[0]

        #broke up the date into year, month, and day for reformatting purposes
        year  = date[-4:]
        day = date[3:5]
        month = date[:2]

        #this formats the date given into a format our group agreed upon
        formatted_date = str(year) + '-' + str(month) + '-' + str(day)

        date_list.append(formatted_date)

    for row in rows[38:63]:

        header = row.find('td')['headers']
        date = header[0]

        #broke up the date into year, month, and day for reformatting purposes
        year  = date[-4:]
        day = date[3:5]
        month = date[:2]

        #this formats the date given into a format our group agreed upon
        formatted_date = str(year) + '-' + str(month) + '-' + str(day)

        date_list.append(formatted_date)

    for row in rows[63:-42]:

        header = row.find('td')['headers']
        date = header[0]

        #broke up the date into year, month, and day for reformatting purposes
        year  = date[-4:]
        day = date[3:5]
        month = date[:2]

        #this formats the date given into a format our group agreed upon
        formatted_date = str(year) + '-' + str(month) + '-' + str(day)

        date_list.append(formatted_date)
        
    return date_list


def get_national_initial_nsa_claims():
    #Gets weekly initial not seasonally adjusted jobless claims from week of March 14, 2020 to March 13, 2021. 
    #Function will return a list of these numbers per week
    #List returns numbers chronologically from March 2020 to may 2020

    nsa_list = []

    anchor = soup.find("table")
    rows = anchor.find_all('tr')

    #rows is sliced to get data from specified dates our group decided upon
    for row in rows[13:38]:

        nsa_initial_claim_num = row.find('td').text

        #this gets rid of commas in numbers so I can convert the strings into integers without error
        num = nsa_initial_claim_num.replace(',', '')  

        nsa_list.append(int(num))

    for row in rows[38:63]:

        nsa_initial_claim_num = row.find('td').text

        #this gets rid of commas in numbers so I can convert the strings into integers without error
        num = nsa_initial_claim_num.replace(',', '')  

        nsa_list.append(int(num))

    for row in rows[63:-42]:

        nsa_initial_claim_num = row.find('td').text

        #this gets rid of commas in numbers so I can convert the strings into integers without error
        num = nsa_initial_claim_num.replace(',', '')  

        nsa_list.append(int(num))

    return nsa_list

def get_mich_initial_nsa_claims():

    #Gets weekly initial not seasonally adjusted jobless claims from week of March 14, 2020 to March 13, 2021. 
    #Function will return a list of these numbers per week
    #List returns numbers chronologically from March 2020 to may 2020

    with open("mich_unemployment.html") as f:
        soup = BeautifulSoup(f, "html.parser")

    nsa_list = []

    anchor = soup.find("table")
    rows = anchor.find_all('tr')

    #rows is sliced to get data from specified dates our group decided upon
    for row in rows[12:37]:

        nsa_initial_claim_num = row.find('td').text

        #this gets rid of commas in numbers so I can convert the strings into integers without error
        num = nsa_initial_claim_num.replace(',', '')  

        nsa_list.append(int(num))

    for row in rows[37:62]:

        nsa_initial_claim_num = row.find('td').text

        #this gets rid of commas in numbers so I can convert the strings into integers without error
        num = nsa_initial_claim_num.replace(',', '')  

        nsa_list.append(int(num))

    for row in rows[62:-4]:

        nsa_initial_claim_num = row.find('td').text

        #this gets rid of commas in numbers so I can convert the strings into integers without error
        num = nsa_initial_claim_num.replace(',', '')  

        nsa_list.append(int(num))

    return nsa_list

def get_national_weekly_cumulative_total():
    #Function gets a weekly cumulative total of initial NSA unemployment claims as weeks go on
    #Function will return a list of the cumulative total per week 
    #List returns numbers chronologically from March 2020 to may 2020
    total = 0 

    cumulative_totals = []
    claim_per_week = get_national_initial_nsa_claims()
    
    for num in claim_per_week:
        total += num
        cumulative_totals.append(total)

    return cumulative_totals

def get_mich_weekly_cumulative_total():
    #Function gets a weekly cumulative total of initial Michigan NSA unemployment claims as weeks go on
    #Function will return a list of the cumulative total per week 
    #List returns numbers chronologically from March 2020 to may 2020
    total = 0 

    cumulative_totals = []
    claim_per_week = get_mich_initial_nsa_claims()
    
    for num in claim_per_week:
        total += num
        cumulative_totals.append(total)

    return cumulative_totals
    
def make_national_dict():
    #Function calls get_week_dates and get_national_initial_nsa_claims 
    #Function will turn both lists that are returned into a dictionary
    #Dates will be the keys and initial NSA claims for national data will be values
    #Function will return the dictionary

    dates = get_week_dates()
    claim_nums = get_national_initial_nsa_claims()

    return dict(zip(dates, claim_nums))

def make_mich_dict():
    #Function calls get_week_dates and get_mich_initial_nsa_claims 
    #Function will turn both lists that are returned into a dictionary
    #Dates will be the keys and initial NSA claims for Michigan data will be values
    #Function will return the dictionary

    dates = get_week_dates()
    claim_nums = get_mich_initial_nsa_claims()

    return dict(zip(dates, claim_nums))


def fill_database(nat_unemployment_dict, mich_unemployment_dict, nat_totals_list, mich_totals_list, cur, conn):

    #creates weeks table
    #cur.execute('''CREATE TABLE IF NOT EXISTS weeks (id INTEGER PRIMARY KEY, week TEXT UNIQUE)''')

    #creates table of unemployment rates
    cur.execute('''CREATE TABLE IF NOT EXISTS unemployment_rates (i INTEGER PRIMARY KEY, week INTEGER, loc INTEGER, initial_nsa_claims INTEGER, total_claims INTEGER)''')
    

    for x in range(len(nat_unemployment_dict.keys())):
        weeks = list(nat_unemployment_dict.keys())

        #creates variables to insert into table
        week_key = weeks[x]
        unemployment_claim_num = nat_unemployment_dict[week_key]
        nat_total = nat_totals_list[x]
        week_num = x + 1

        loc = "MI"
        #get location id
        cur.execute('''SELECT id FROM locs where loc = (?)''', (loc,))
        loc_id = cur.fetchone()[0]

        #adds information into weeks table
        cur.execute('''INSERT OR IGNORE INTO Weeks (id, week) VALUES (?, ?)''', (week_num, week_key))

        #adds information into unemployment rates table
        cur.execute('''INSERT OR IGNORE INTO unemployment_rates (i, week, loc, initial_nsa_claims, total_claims) VALUES (?, ?, ?, ?, ?)''', (x, week_num, loc_id, unemployment_claim_num, nat_total))

    for x in range(len(mich_unemployment_dict.keys())):

        weeks = list(mich_unemployment_dict.keys())

        #creates variables to insert into table
        week_key = weeks[x]

        mich_unemployment_claim_num = mich_unemployment_dict[week_key]

        mich_total = mich_totals_list[x]

        week_num = x + 1

        loc = "MI"

        cur.execute('''SELECT id FROM locs where loc = (?)''', (loc,))

        loc_id = cur.fetchone()[0]


        #adds information into unemployment rates table
        cur.execute('''INSERT OR IGNORE INTO unemployment_rates (i, week, loc, initial_nsa_claims, total_claims) VALUES (?, ?, ?, ?, ?)''', (x, week_num, loc_id, mich_unemployment_claim_num, mich_total))
    
    
    print("Finished adding data")
    conn.commit()


def main():
    '''Calls functions to get data from the CDC, BTS, and Unemployment Bureau.
    Function takes in no inputs and does not return anything''' 

    #PART 1: Collect and insert mobility data
    t = datetime.datetime(2020, 3, 15)
    nats = build_national_lst(t)
    mi = build_state_lst(t)
    create_transport_database(nats, mi)

    #PART 2: Collect and insert COVID-19 deaths data

    midata = get_data()[0]
    usdata = get_data()[1]
    create_db(midata,usdata)

    #PART 3: Collect and insert unemployment claims data
    cur, conn = setUpDatabase('full_data.db')

    weekly_national_umemployment_claims_dict = make_national_dict()
    weekly_mich_umemployment_claims_dict = make_mich_dict()

    cumulative_national_claims_list = get_national_weekly_cumulative_total()
    cumulative_mich_claims_list = get_mich_weekly_cumulative_total()
    fill_database(weekly_national_umemployment_claims_dict, weekly_mich_umemployment_claims_dict, cumulative_national_claims_list, cumulative_mich_claims_list, cur, conn)

    #Close connection
    conn.close()

if __name__ == "__main__":
    main()
