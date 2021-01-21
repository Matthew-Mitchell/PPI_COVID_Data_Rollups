import requests
import pandas as pd
import matplotlib.pyplot as plt
import datetime

#Read in NYU Data
df = pd.read_csv('https://raw.githubusercontent.com/publicsafetylab/public-psl-jdi-pops/master/data.csv')


orig_len = len(df)


#Reformat Date Column to be Timestamp
df['Scrape_Date'] = pd.to_datetime(df['Scrape_Date'])
df = df[df['Scrape_Date'] >= pd.to_datetime("03-10-2020")]
after_mar10 = len(df)


print("{} post march 10th.".format(after_mar10/orig_len))


## Subset to Jails with >75% days populated with data



ndata_per_jail = df.groupby('STATE-COUNTY')['Population'].count()
max_ex = max(ndata_per_jail)
threshold = .75*max_ex
jails_to_include = ndata_per_jail[ndata_per_jail>=threshold].index

df = df[df['STATE-COUNTY'].isin(jails_to_include)]
over_75percent_data = len(df)



## Quantifying Data Drop-Off


percent = str(round(len(jails_to_include)/len(ndata_per_jail),2)*100)
message = "{} of {} jails ({}%) have over 75% data populated post march 10th."
print(message.format(len(jails_to_include),len(ndata_per_jail), percent))


## Jails with the last date needs to be within 3 months

today = pd.to_datetime(datetime.datetime.now().strftime('%m-%d-%Y')) 
three_months_ago = today - datetime.timedelta(days = 90)


jails_most_recent = df.groupby('STATE-COUNTY')['Scrape_Date'].max()
jails_to_include = jails_most_recent[jails_most_recent>=three_months_ago].index
df = df[df['STATE-COUNTY'].isin(jails_to_include)] #Subset master dataset


## Quantifying Drop Off Again: Generally have seen that all jails that have 75% of data populated also include data from within 3 months:
within_3_months = len(df)
within_3_months / over_75percent_data

## Create Month / Day of Week / Year Columns 
df['Month'] = df.Scrape_Date.dt.strftime('%m')
df['Year'] = df.Scrape_Date.dt.strftime('%Y')
df['DayOfWeek'] = df.Scrape_Date.dt.strftime('%A')
#Convert Datetime to Strict Date
df['Scrape_Date'] = df['Scrape_Date'].dt.date

## Summarizing Daily Jail Populations

jail_totals = df.groupby('Scrape_Date')['Population'].agg(['count', 'sum'])
jail_totals.columns = ['Count_Of_Jails','Total_Jail_Population']
jail_totals['Seven_Day_Rolling_Average'] = jail_totals['Total_Jail_Population'].rolling(7).mean()


## First Day Available of Each Month
first_days = df.groupby(['STATE-COUNTY', 'Month', 'Year']).head(1)
first_days['Scrape_Date'] = "FirstDay" + first_days['Month'] + "-" + first_days['Year']


## First Monday of Each Month

mondays = df[df.DayOfWeek=='Monday']
first_mondays = mondays.groupby(['STATE-COUNTY', 'Month', 'Year']).head(1)


most_recent = df.groupby(['STATE-COUNTY']).tail(1)


## Verify DataFrame Correctly Sorted by Facility/Date
assert(all(df == df.sort_values(by=['STATE-COUNTY', 'Scrape_Date'])))
## Ensure there is one per Facility
assert(len(most_recent) == df.STATE-COUNTY.nunique())


## March 10th

march_10 = df[df.Scrape_Date == pd.to_datetime("03-10-2020")]

## First Mondays Summary
first_monday_summary = first_mondays.groupby(['STATE-COUNTY', 'Month', 'Year']).head(1)
first_monday_summary['Scrape_Date'] = "FirstMondayAvailable" + first_monday_summary['Month'] + "-" + first_monday_summary['Year']

# Stitching Together the Final Summary
pre_pivot = pd.concat([march_10, first_mondays, most_recent, first_monday_summary, first_days], ignore_index=True)
# Remove Potential Duplicates (First Mondays and Most Recent could include duplicates)
pre_pivot = pre_pivot.drop_duplicates(subset=['STATE-COUNTY', 'Scrape_Date'])
final = pre_pivot.pivot(index="STATE-COUNTY", columns="Scrape_Date", values="Population")


## Save Transformed Output to Excel

today = datetime.datetime.now().strftime('%m-%d-%Y') #Create string of today's date

filename = "Jail_Summaries_as_of_{}.xlsx".format(today)
print('Saving to: {}'.format(filename))

with pd.ExcelWriter(filename) as writer1:
    to_save = [(final, 'FirstMondays'),(jail_totals, 'DailyJailTotals')]
    #final.to_excel(writer1, sheet_name = 'FirstMondays')
    #jail_totals.to_excel(writer1, sheet_name = 'DailyJailTotals')
    #Fix Column Widths
    for df, sheetname in to_save:
        df.to_excel(writer1, sheet_name = sheetname)
        worksheet = writer1.sheets[sheetname]  # pull worksheet object
        for idx, col in enumerate(df):  # loop through all columns
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 12  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width

