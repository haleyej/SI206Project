import requests
from bs4 import BeautifulSoup
import re
import os
import csv
import sqlite3
import json

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
    for row in rows[13:-42]:

        header = row.find('td')['headers']
        date = header[0]

        #broke up the date into year, month, and day for reformatting purposes
        year = date[-4:]
        day = date[3:5]
        month = date[:2]

        #this formats the date given into a format our group agreed upon
        formatted_date = str(year) + '-' + str(month) + '-' + str(day)

        date_list.append(formatted_date)
        
    return date_list


def get_initial_nsa_claims():

    #Gets weekly initial not seasonally adjusted jobless claims from week of March 14, 2020 to March 13, 2021. 
    #Function will return a list of these numbers per week
    #List returns numbers chronologically from March 2020 to may 2020

    nsa_list = []

    anchor = soup.find("table")
    rows = anchor.find_all('tr')

    #rows is sliced to get data from specified dates our group decided upon
    for row in rows[13:-42]:

        nsa_initial_claim_num = row.find('td').text

        #this gets rid of commas in numbers so I can convert the strings into integers without error
        num = nsa_initial_claim_num.replace(',', '')  

        nsa_list.append(int(num))

    return nsa_list

def get_weekly_cumulative_total():
    #Function gets a weekly cumulative total of initial NSA unemployment claims as weeks go on
    #Function will return a list of the cumulative total per week 
    #List returns numbers chronologically from March 2020 to may 2020
    total = 0 

    cumulative_totals = []
    claim_per_week = get_initial_nsa_claims()
    
    for num in claim_per_week:
        total += num
        cumulative_totals.append(total)

    return cumulative_totals
    
def make_dict():
    #Function calls get_week_dates and get_initial_nsa_claims 
    #Function will turn both lists that are returned into a dictionary
    #Dates will be the keys and initial NSA claims will be values
    #Function will return the dictionary

    dates = get_week_dates()
    claim_nums = get_initial_nsa_claims()

    return dict(zip(dates, claim_nums))

def create_database(database_name):
    #Creates a database to store information in

    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+database_name)
    cur = conn.cursor()

    return (cur, conn)

def fill_database(umemployment_dict, totals_list):

    cur, conn = setUpDatabase('unemployment_rates.db')

    #creates weeks table
    cur.execute('''CREATE TABLE IF NOT EXISTS Weeks (id INTEGER PRIMARY KEY, week TEXT UNIQUE)''')

    #creates table of unemployment rates
    cur.execute("CREATE TABLE IF NOT EXISTS unemployment_rates (week INTEGER PRIMARY KEY, initial_nsa_claims INTEGER, total_claims INTEGER)"
    

    for x in range(len(umemployment_dict.keys())):
        weeks = list(umemployment_dict.keys())

        #creates variables to insert into table
        week_key = week[x]
        unemployment_claim_num = umemployment_dict[week_key]
        total = totals_list[x]
        week_num = i + 1

        #adds information into weeks table
        cur.execute('''INSERT OR IGNORE INTO Weeks (week) VALUES (?)''', (week_num, week_key))
        cur.execute('''SELECT id FROM Weeks where week = (?)''', (week_num,))

        week_id = cur.fetchone()[0]

        #adds information into unemployment rates table
        cur.execute('''INSERT OR IGNORE INTO weeks (week, initial_nsa_claims) VALUES (?, ?, ?)''', (week_id, unemployment_claim_num, total))
    
    print("Finished adding data")
    conn.commit()






def main():
    """Takes nothing as an input and returns nothing. Calls the functions make_dict(), get_weekly_cumlative_total(), create_database(), and fill_database(). Closes the database connection."""

    weekly_umemployment_claims_dict = make_dict()
    cumulative_claims_list = get_weekly_cumulative_total()

    cur, conn = create_database('unemployment_tables.db')
    fill_database(weekly_umemployment_claims_dict, cumulative_claims_list)

    conn.close()


if __name__ == "__main__":
    main()