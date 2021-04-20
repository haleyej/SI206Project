import requests
import json
import os
import sqlite3
import praw
import time
import calendar
import datetime
from datetime import date, timedelta

#pip install praw


def create_user_agent():
    reddit = praw.Reddit(
        client_id="FRs6KuNO_pBZMg",
        client_secret="ZEaULMdE9y9taHyX7Bavbk4HZVtZvw",
        user_agent="SI206",
)

def get_resp(sub, search, limit = 25, listing = 'top'):
    '''Get a response from the reddit API. Searches a specific subreddit for a topic. If you want to look at Reddit more generally,
    use "all" for the sub. 
    Options for listing: controversial, new, top, relevant'''
    try: 
        url = f"https://www.reddit.com/r/{sub}/{listing}.json?limit={limit}&q={search}&t=alltime&self=true"
        resp = requests.get(url, headers = {'User-agent': 'SI206'})
        content = json.loads(resp.text)
        return content
    except:
        return('Invalid URL, please try again')

def get_data(sub, search, start_date, limit = 25, listing = 'top', data_dict = {'search':[]}):
    '''Takes in a bunch of search parameters, returns a dictionary. Calls get_resp
    Keys include search (list with one item, the search term used to get the response) and a dictionary of data for a week'''
    data_dict['search'].append(search)
    resp = get_resp(sub, search, limit, listing)
    tot_upvotes = 0
    tot_awards = 0
    tot_downvotes = 0
    tot_items = 0
    for item in resp['data']['children']:
        time = item['data']['created_utc']
        week = find_week(time)
        week = week.split("-")
        week = datetime.datetime(int(week[0]), int(week[1]), int(week[2]))
    if start_date <= week: 
        week = str(week).split()[0]
        tot_items += 1
        if week not in data_dict:
            data_dict[week] = {}
            data_dict[week]['upvotes'] = item['data']['ups']
            data_dict[week]['awards'] = item['data']['total_awards_received']
        elif week in data_dict:
            data_dict[week]['upvotes'] += item['data']['ups']
            data_dict[week]['awards'] += item['data']['total_awards_received']
        data_dict[week]['sampled_posts'] = tot_items
        data_dict[week]['average_upvotes'] = round(data_dict[week]['upvotes'] / tot_items, 4)
        data_dict[week]['average_awards'] = round(data_dict[week]['awards'] / tot_items, 4)
    return data_dict



def find_week(reddit_time):
    '''Uses datetime modules to find the week from epoch time (reddit api returns epoch time) '''
    real_time = datetime.datetime.utcfromtimestamp(reddit_time).replace(tzinfo=datetime.timezone.utc)
    week = real_time - timedelta(days=real_time.weekday())
    week = str(week).split()[0]
    return week
    

    
def create_searches_database(data_base_name):
    '''Creates a database'''
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+data_base_name)
    cur = conn.cursor()
    return (cur, conn)


def fill_searches_database(search, start_month, start_day, start_year, cur, conn, sub = 'all', limit = 25, listing = 'top', runs = 1):
    '''This code takes in a bunch of search parameters. Basically everything happens in this function to get the data and fill the database
    Calls get_data, does NOT call create_searches_database(), that function needs to be called before'''
    print("Putting information in database for search", search, "in the subreddit r/", sub)
    start_date = datetime.datetime(start_year, start_month, start_day)
    cur.execute('''CREATE TABLE IF NOT EXISTS Reddit (week TEXT, search INTEGER, sampled_posts INTEGER,
                    total_upvotes INTEGER, total_awards INTEGER, average_upvotes INTEGER, average_awards INTEGER)''')
    data = get_data(search = search, sub = sub, start_date = start_date, limit = limit, listing = listing)
    ##create serches table
    searches_lst = data['search']
    cur.execute("CREATE TABLE IF NOT EXISTS Searches (id INTEGER PRIMARY KEY, search TEXT UNIQUE)")
    cur.execute("INSERT OR IGNORE INTO Searches (search) VALUES (?)",(searches_lst[-1],))
    conn.commit()
    ##create weeks table
    weeks = list(data.keys())[1:]
    cur.execute('''CREATE TABLE IF NOT EXISTS Weeks (id INTEGER PRIMARY KEY, week TEXT UNIQUE)''')
    ##fill table
    current_search = searches_lst[-1]
    cur.execute("SELECT id FROM Searches where search = (?)", (current_search,))
    search_int = cur.fetchone()[0]
    for i in range(len(list(data.keys())[1:])):
        current_week = str(weeks[i])
        #Put week in weeks table
        cur.execute('''INSERT OR IGNORE INTO Weeks (week) VALUES (?)''', (current_week,))
        cur.execute('''SELECT id FROM Weeks where week = (?)''', (current_week,))
        week_id = cur.fetchone()[0]
        cur.execute('''INSERT OR IGNORE INTO Reddit (week, search, sampled_posts, total_upvotes, total_awards, average_upvotes, average_awards)
                    VALUES (?, ?, ?, ?, ?, ?, ?)''', (week_id, search_int, data[current_week]['sampled_posts'], data[current_week]['upvotes'], 
                    data[current_week]['awards'], data[current_week]['average_upvotes'], data[current_week]['average_awards']))
    conn.commit()
    print("Finished putting information in database for search", search)



def main():
    print('executing')
    cur, conn = create_searches_database('Reddit.db')

    #sample searches to fill database
    fill_searches_database('COVID', 3, 15, 2020, cur, conn)
    fill_searches_database('Coronavirus', 4, 15, 2021, cur, conn)
    fill_searches_database('COVID', 3, 15, 2020, cur, conn, listing = 'controversial')
    fill_searches_database('Vaccine', 3, 15, 2020, cur, conn)
    fill_searches_database('lockdown', 3, 15, 2020, cur, conn)
    fill_searches_database('lockdown', 3, 15, 2020, cur, conn, listing = 'controversial')
    fill_searches_database('quarantine', 3, 15, 2020, cur, conn)
    fill_searches_database('Dr.Fauci', 3, 15, 2020, cur, conn)
    fill_searches_database('World Health Organization', 3, 15, 2020, cur, conn)
    fill_searches_database("COVID", 3, 15, 20, cur, conn, sub = 'Michigan')
    fill_searches_database("COVID", 3, 15, 20, cur, conn, sub = 'Michigan', listing = 'controversial')
    fill_searches_database("Anti mask", 3, 15, 20, cur, conn, sub = 'news', listing = 'controversial')

    conn.close()




if __name__ == "__main__":
    main()

