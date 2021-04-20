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
    
def make_dict():
    #Function calls get_week_dates and get_initial_nsa_claims 
    #Function will turn both lists that are returned into a dictionary
    #Dates will be the keys and initial NSA claims will be values
    #Function will return the dictionary

    dates = get_week_dates()
    claim_nums = get_initial_nsa_claims()

    return dict(zip(dates, claim_nums))



def main():
    """Takes nothing as an input and returns nothing. Calls the functions setUpDatabase(), set_up_tables(), set_up_artist_id_table(), fill_up_hot_100_table(), and write_data_to_file(). Closes the database connection."""
    print(make_dict())

if __name__ == "__main__":
    main()