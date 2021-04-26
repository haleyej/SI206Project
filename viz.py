import matplotlib
import matplotlib.pyplot as plt 
import sqlite3
import os

# set up the cursor
dir = os.path.dirname(__file__)+os.sep
conn = sqlite3.connect(dir+'full_data.db')
cur = conn.cursor()

#-------------------------------------------------TRANSPORTATION DATA -------------------------------------------------
USathome=[]
results = cur.execute('SELECT at_home FROM transport WHERE location_id =1 ORDER BY week_id asc').fetchall()
for num in results:
    num = num[0]
    USathome.append(num)
USnotathome = []
results = cur.execute('SELECT not_at_home FROM transport WHERE location_id =1 ORDER BY week_id asc').fetchall()
for num in results:
    num = num[0]
    USnotathome.append(num)
USathomeratio=[]
results = cur.execute('SELECT ratio_at_home FROM transport WHERE location_id =1 ORDER BY week_id asc').fetchall()
for num in results:
    num = num[0]
    USathomeratio.append(num)
MIathome=[]
results = cur.execute('SELECT at_home FROM transport WHERE location_id =2 ORDER BY week_id asc').fetchall()
for num in results:
    num = num[0]
    MIathome.append(num)
MInotathome = []
results = cur.execute('SELECT not_at_home FROM transport WHERE location_id =2 ORDER BY week_id asc').fetchall()
for num in results:
    num = num[0]
    MInotathome.append(num)
MIathomeratio=[]
results = cur.execute('SELECT ratio_at_home FROM transport WHERE location_id =2 ORDER BY week_id asc')
for num in results:
    num = num[0]
    MIathomeratio.append(num)

#-------------------------------------------------UNEMPLOYMENT DATA-------------------------------------------------
new_unemploy_MI =[]
results = cur.execute('SELECT initial_nsa_claims FROM unemployment_rates WHERE loc =2 ORDER BY week asc').fetchall()
for num in results:
    num = num[0]
    new_unemploy_MI.append(num)

total_unemploy_MI =[]
results = cur.execute('SELECT total_claims FROM unemployment_rates WHERE loc =2 ORDER BY week asc').fetchall()
for num in results:
    num = num[0]
    total_unemploy_MI.append(num)

new_unemploy_US=[]
results = cur.execute('SELECT initial_nsa_claims FROM unemployment_rates WHERE loc =1 ORDER BY week asc').fetchall()
for num in results:
    num = num[0]
    new_unemploy_US.append(num)

total_unemploy_US=[]
results = cur.execute('SELECT total_claims FROM unemployment_rates WHERE loc =1 ORDER BY week asc').fetchall()
for num in results:
    num = num[0]
    total_unemploy_US.append(num)

#-------------------------------------------------COVID DEATHS-------------------------------------------------
covid_deaths_MI=[]
results = cur.execute('SELECT deaths FROM covid_deaths WHERE loc =2 ORDER BY wk asc').fetchall()
for num in results:
    num = num[0]
    covid_deaths_MI.append(num)

covid_deaths_US=[]
results = cur.execute ('SELECT deaths FROM covid_deaths WHERE loc =1 ORDER BY wk asc').fetchall()
for num in results:
    num = num[0]
    covid_deaths_US.append(num)
#-------------------------------------------------VISUALIZATIONS-------------------------------------------------
weeks =[]
count =0
for i in range(53):
    count +=1
    weeks.append(count)

#1 - US vs MI athome ratio
fig,ax = plt.subplots()
y1 = MIathomeratio
y2 = USathomeratio
ax.plot(weeks,y1, "g-", label = "Michigan")
ax.plot(weeks, y2, 'b-', label = "National")
ax.set_xlabel('Week')
ax.set_ylabel('Ratio of # people at-home / # not at-home')
ax.set_title('Ratio of People At-Home vs Not: Nationally and in MI')
ax.grid()
ax.legend()


#2 new unemployment rates & covid deaths

fig= plt.figure(figsize = (15,8))
ax1=fig.add_subplot(121)
ax2 = fig.add_subplot(122)
ax1.scatter(covid_deaths_MI, new_unemploy_MI, color = "g")
ax1.set_xlabel('MI Weekly Covid Deaths')
ax1.set_ylabel('New MI Unemployment Claims')
ax1.grid()
ax1.set_title('Covid Deaths by New Unemployment Claims: Michigan')

ax2.scatter(covid_deaths_US, new_unemploy_US,color = "b")
ax2.set_xlabel('National Weekly Covid Deaths')
ax2.set_ylabel('New National Unemployment Claims')
ax2.grid()
ax2.set_title('Covid Deaths by New Unemployment Claims: National')

#3 - at home ratio & covid deaths 
fig2= plt.figure(figsize = (15,8))
ax1=fig2.add_subplot(121)
ax2 = fig2.add_subplot(122)
ax1.scatter(USnotathome[:-1], covid_deaths_US, color = "purple")
ax1.set_ylabel('National Weekly Covid Deaths')
ax1.set_xlabel('People Not At-Home: National')
ax1.grid()
ax1.set_title('Weekly Covid Deaths by # of People Not at Home: National')

ax2.scatter(MInotathome[:-1],covid_deaths_MI, color = "red")
ax2.set_ylabel('MI Weekly Covid Deaths')
ax2.set_xlabel('People Not At-Home: Michigan')
ax2.grid()
ax2.set_title('Weekly Covid Deaths by # of People Not at Home: Michigan')

# show graphs
plt.show()