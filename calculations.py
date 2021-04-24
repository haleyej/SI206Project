from covid_data import setUpDatabase
import csv

cur, conn = setUpDatabase('full_data.db')

cur.execute('''SELECT covid_deaths.deaths, covid_deaths.loc, transport.location_id, transport.at_home, transport.ratio_at_home
                FROM transport
                JOIN covid_deaths
                ON transport.week_id = covid_deaths.wk''')
# data = cur.fetchall()
# for d in data:
#     print(d)
# conn.commit()



def calculate_covid_death_national_percent_change(cur, conn):
    '''Gets the percent change per week of national covid deaths
    Returns a list chronologically per week'''

    cur.execute('''SELECT covid_deaths.deaths
                    FROM covid_deaths
                    WHERE covid_deaths.loc = "1" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change

def calculate_covid_death_mich_percent_change(cur, conn):
    '''Gets the percent change per week of Michigan covid deaths
    Returns a list chronologically per week'''

    cur.execute('''SELECT covid_deaths.deaths
                    FROM covid_deaths
                    WHERE covid_deaths.loc = "2" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change

def calculate_cumulative_covid_death_nationally(cur, conn):
    '''Gets the cumulative COVID death total per week of National covid deaths
    Returns a list chronologically per week'''

    cur.execute('''SELECT covid_deaths.deaths
                    FROM covid_deaths
                    WHERE covid_deaths.loc = "1" ''')
    data = cur.fetchall()
    conn.commit()

    cumulative_totals = []
    total = 0

    for num in data:
        total += num[0]
        cumulative_totals.append(total)

    return cumulative_totals

def calculate_cumulative_covid_death_mich(cur, conn):
    '''Gets the cumulative COVID death total per week of Michigan covid deaths
    Returns a list chronologically per week'''

    cur.execute('''SELECT covid_deaths.deaths
                    FROM covid_deaths
                    WHERE covid_deaths.loc = "2" ''')
    data = cur.fetchall()
    conn.commit()

    cumulative_totals = []
    total = 0

    for num in data:
        total += num[0]
        cumulative_totals.append(total)

    return cumulative_totals

def calculate_national_not_home_percent_change(cur, conn):
    '''Gets the percent change per week of people not at home nationally
    Returns a list chronologically per week'''

    cur.execute('''SELECT transport.not_at_home
                    FROM transport
                    WHERE transport.location_id = "1" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change

def calculate_national_home_percent_change(cur, conn):
    '''Gets the percent change per week of people at home nationally
    Returns a list chronologically per week'''

    cur.execute('''SELECT transport.at_home
                    FROM transport
                    WHERE transport.location_id = "1" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change

def calculate_mich_not_home_percent_change(cur, conn):
    '''Gets the percent change per week of people not at home in Michigan
    Returns a list chronologically per week'''

    cur.execute('''SELECT transport.not_at_home
                    FROM transport
                    WHERE transport.location_id = "2" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change

def calculate_mich_home_percent_change(cur, conn):
    '''Gets the percent change per week of people at home in Michigan
    Returns a list chronologically per week'''

    cur.execute('''SELECT transport.at_home
                    FROM transport
                    WHERE transport.location_id = "2" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change

def calculate_national_unemployment_percent_change(cur, conn):
    '''Gets the percent change per week of unemployment claims nationally
    Returns a list chronologically per week'''

    cur.execute('''SELECT unemployment_rates.initial_nsa_claims
                    FROM unemployment_rates
                    WHERE unemployment_rates.loc = "1" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change


def calculate_mich_unemployment_percent_change(cur, conn):
    '''Gets the percent change per week of unemployment claims in Michigan
    Returns a list chronologically per week'''

    cur.execute('''SELECT unemployment_rates.initial_nsa_claims
                    FROM unemployment_rates
                    WHERE unemployment_rates.loc = "1" ''')
    data = cur.fetchall()
    conn.commit()

    percent_change = []

    for x in range(len(data)-1):
        first = data[x][0]
        second = data[x+1][0]

        increase = second - first
        percent = increase / first * 100

        percent_change.append(percent)

    return percent_change

def write_to_csv(nat_cd_pc, mich_cd_pc, nat_cd_cum, mich_cd_cum, nat_nh_pc, nat_h_pc, mich_nh_pc, mich_h_pc, nat_unemp_pc, mich_unemp_pc):
    with open("calculations.csv", "w", newline = "") as f:
            csvw = csv.writer(f)

            header = ["Week Number", "National Covid Death Percent Change", "Michigan Covid Death Percent Change", "National Cumulative Covid Deaths", "Michigan Cumulative Covid Deaths", "National People Not Home Percent Change",
            "National People Home Percent Change", "Michigan People Not Home Percent Change", "Michigan People Home Percent Change", "National Unemployment Claim Percent Change", "Michigan Unemployment Claim Percent Change" ]

            csvw.writerow(header)

            for x in range(51):

                week = x + 1
                ncdpc = nat_cd_pc[x]
                mcdpc = mich_cd_pc[x]
                ncdcum = nat_cd_cum[x]
                mcdcum = mich_cd_cum[x]
                nnhpc = nat_nh_pc[x]
                nhpc = nat_h_pc[x]
                mnhpc = mich_nh_pc[x]
                mhpc = mich_h_pc[x]
                nuepc =nat_unemp_pc[x]
                muepc = mich_unemp_pc[x]

                data = [week, ncdpc, mcdpc, ncdcum, mcdcum, nnhpc, nhpc, mnhpc, mhpc, nuepc, muepc]
                csvw.writerow(data)





def main():
    cur, conn = setUpDatabase('full_data.db')

    nat_cd_pc = calculate_covid_death_national_percent_change(cur, conn)

    mich_cd_pc =calculate_covid_death_mich_percent_change(cur, conn)

    nat_cd_cum = calculate_cumulative_covid_death_nationally(cur, conn)

    mich_cd_cum = calculate_cumulative_covid_death_mich(cur, conn)

    nat_nh_pc =calculate_national_not_home_percent_change(cur, conn)
    
    nat_h_pc = calculate_national_home_percent_change(cur, conn)

    mich_nh_pc = calculate_mich_not_home_percent_change(cur, conn)
    
    mich_h_pc = calculate_mich_home_percent_change(cur, conn)

    nat_unemp_pc = calculate_national_unemployment_percent_change(cur, conn)

    mich_unemp_pc = calculate_mich_unemployment_percent_change(cur, conn)

    

    write_to_csv(nat_cd_pc, mich_cd_pc, nat_cd_cum, mich_cd_cum, nat_nh_pc, nat_h_pc, mich_nh_pc, mich_h_pc, nat_unemp_pc, mich_unemp_pc)




    conn.close()

if __name__ == "__main__":
    main()