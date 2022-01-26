import requests
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import glob
import numpy as np
import re

#Read in NYU Data
# df = pd.read_csv('https://raw.githubusercontent.com/publicsafetylab/public-psl-jdi-pops/master/data.csv')
def custom_sort(jdi):
    date = jdi.split("_")[-1].strip(".csv")
    date = pd.to_datetime(date)
    return date

def find_closest_data2date(date, df):
    marker_time = date
    df['Distance2Time'] = df['Scrape_Date'] - marker_time
    df['Distance2Time_ABS'] = df['Distance2Time'].apply(lambda x: (np.abs(x.days)))
    temp = df.sort_values(by=['STATE-COUNTY','Distance2Time_ABS']).groupby(['STATE-COUNTY']).head(1)
    temp = temp.set_index("STATE-COUNTY")[['Population','Scrape_Date']]
    return temp

def rename_snapshot(population_rename, date_rename, temp):
    temp = temp.rename(columns = {"Population":population_rename,
                                 "Scrape_Date":date_rename})
    return temp
# #Find all files of the form jdi_booking_stats_DATE.csv
# jdi_files = glob.glob("jdi_booking_stats_*.csv")
# jdi_files = sorted(jdi_files, key= custom_sort)
# cur_file = jdi_files[-1]
# print("Reading file: ", cur_file)

# df = pd.read_csv(cur_file)
# # cols2drop = ['Unnamed: 0', 'population_zeroed', 'population_raw']
# # df = df.drop(cols2drop, axis=1)
# print(df.columns)

# column_dictionary = {'Population' : 'Population',
#                      'Date':'Scrape_Date'}
# df = df.rename(columns = column_dictionary)
# df['STATE-COUNTY'] = df['State'] + "-" + df["Jail"]
# df = df[['Population', 'Scrape_Date', 'STATE-COUNTY']]

# # df.columns = ['Population', 'Scrape_Date', 'STATE-COUNTY']
# print(df.columns)
# print(df.head(2))
# orig_len = len(df)


# #Reformat Date Column to be Timestamp
# df['Scrape_Date'] = pd.to_datetime(df['Scrape_Date'])
# df = df[df['Scrape_Date'] >= pd.to_datetime("03-10-2020")]

# #Round the linearly interpolated data
# #Custome function for if there are blank values when attempting to round
# def roundORblank(pop):
#     try:
#         pop = round(pop)
#     except:
#         assert np.isnan(pop), "Error did not round and is not null!!"
#     return pop

# try:
#     df['Population'] = df.Population.apply(round) #Use built in rounding
# except:
#     print("""Found Missing Linear Interpolated Populations!!
#     Using Custom Function which will leave these values blank while rounding the rest.""")
#     missing = df[df.Population.isna()]
#     print("Number of missing Population Values since March 10th:", len(missing))
#     print("Preview of missing values:", missing)
#     df['Population'] = df.Population.apply(roundORblank)

# after_mar10 = len(df)


# print("{} post march 10th.".format(after_mar10/orig_len))


# ## Subset to Jails with >75% days populated with data



# ndata_per_jail = df.groupby('STATE-COUNTY')['Population'].count()
# max_ex = max(ndata_per_jail)
# threshold = .75*max_ex
# jails_to_include = ndata_per_jail[ndata_per_jail>=threshold].index

# df = df[df['STATE-COUNTY'].isin(jails_to_include)] #Subset to jails with 75% data populated compared to max number records for single facility
# over_75percent_data = len(df)

# march10_facilities = df[(df.Scrape_Date == "2020-03-10")
#                         & (~df.Population.isna())]['STATE-COUNTY'].unique()
# df = df[df['STATE-COUNTY'].isin(march10_facilities)] #Subset to jails with data for March 10th 2020



# ## Quantifying Data Drop-Off


# percent = str(round(len(jails_to_include)/len(ndata_per_jail),2)*100)
# message = "{} of {} jails ({}%) have over 75% data populated post march 10th."
# print(message.format(len(jails_to_include),len(ndata_per_jail), percent))


# percentM10_AND_75 = str(round(len(march10_facilities)/len(ndata_per_jail),2)*100)
# message = "{} of {} jails ({}%) have over 75% data populated AND data for March 10th."
# print(message.format(len(march10_facilities),len(ndata_per_jail), percentM10_AND_75))

# ## Jails with the last date needs to be within 3 months
# # today = pd.to_datetime(datetime.datetime.now().strftime('%m-%d-%Y')) 
# #Jan-8-2022: Since no recent data, Setting Date to previous run time
# today = pd.to_datetime("10-08-2021") 

# three_months_ago = today - datetime.timedelta(days = 90)


# jails_most_recent = df.groupby('STATE-COUNTY')['Scrape_Date'].max()
# jails_to_include = jails_most_recent[jails_most_recent>=three_months_ago].index

# df = df[df['STATE-COUNTY'].isin(jails_to_include)] #Subset master dataset


# ## Quantifying Drop Off Again: Generally have seen that all jails that have 75% of data populated also include data from within 3 months:
# within_3_months = len(df)
# within_3_months / over_75percent_data

# ## Create Month / Day of Week / Year Columns 
# df['Month'] = df.Scrape_Date.dt.strftime('%m')
# df['Year'] = df.Scrape_Date.dt.strftime('%Y')
# df['DayOfWeek'] = df.Scrape_Date.dt.strftime('%A')
# #Convert Datetime to Strict Date
# df['Scrape_Date'] = df['Scrape_Date'].dt.date


# ## Update 12-23-21: Cache to Disk - Will be used to Merge New API DATA in. Afterwards, will load this data from disk and comment out above
# today = datetime.datetime.now().strftime('%m-%d-%Y') #Create string of today's date
# filename = "RawData_03-10-21_to_10-07-21_Generated_{}.csv".format(today)
# print("Saving raw Jail Scrape Data to Disk: {}".format(filename))
# df.to_csv(filename)

#Load Data 03-10-21_to_10-07-21_Generated{}

#Load More Recent API DATA

#Merge Datasets

#Load Merged Dataset Through November
# df = pd.read_csv("Combined_Prior_to_Full_Script_Run_01-09-2022.csv")

#Update 1-26-2022 Load Population Aggregates Transformed file
df = pd.read_csv("Transformed_RawData_From_PopulationAggregateAPIendpoint.csv")

df['Scrape_Date'] = pd.to_datetime(df['Scrape_Date'])


#Subset to original Jail List
original = pd.read_csv("RawData_03-10-21_to_10-07-21_Generated_01-09-2022.csv")
df = df[df['STATE-COUNTY'].isin(original['STATE-COUNTY'].unique())]

assert df['STATE-COUNTY'].nunique() == 415, "Error there are no longer 415 Unique State County Jails in the dataset!"
print("Unique State Counties: ", df['STATE-COUNTY'].nunique())

#Ensure Deduplicated
df = df[~df.duplicated(subset=['Scrape_Date','STATE-COUNTY'])]
# Continuation of Previous Scripts

## Summarizing Daily Jail Populations
## This is the second tab of the saved worksheet.
jail_totals = df.groupby('Scrape_Date')['Population'].agg(['count', 'sum'])
jail_totals.columns = ['Count_Of_Jails','Total_Jail_Population']
jail_totals['Seven_Day_Rolling_Average'] = jail_totals['Total_Jail_Population'].rolling(7).mean()


## First Day Available of Each Month
## Ensure Sorted from earliest to most recent
df = df.sort_values(by=['STATE-COUNTY', 'Scrape_Date'])

first_days = df.groupby(['STATE-COUNTY', 'Month', 'Year']).head(1)
first_days = first_days.copy() ##Create copy to avoid warning since first_days is a slice of df
first_days['Scrape_Date'] = "FirstDay" + first_days['Month'].astype(str) + "-" + first_days['Year'].astype(str)


## First Monday of Each Month

mondays = df[df.DayOfWeek=='Monday']
first_mondays = mondays.groupby(['STATE-COUNTY', 'Month', 'Year']).head(1)


most_recent = df.groupby(['STATE-COUNTY']).tail(1)


## Verify DataFrame Correctly Sorted by Facility/Date

assert(all(df == df.sort_values(by=['STATE-COUNTY', 'Scrape_Date'])))
## Ensure there is one per Facility
# assert(len(most_recent) == df.STATE-COUNTY.nunique()) #Deprecated; would need to populate state/county by splitting new state-county field which is true by definition.


## March 10th

march_10 = df[df.Scrape_Date == pd.to_datetime("03-10-2020")]

## First Mondays Summary
first_monday_summary = first_mondays.groupby(['STATE-COUNTY', 'Month', 'Year']).head(1)
# first_monday_summary['Scrape_Date'] = "FirstMondayAvailable" + first_monday_summary['Month'] + "-" + first_monday_summary['Year']
#Update debugging Jan 11th - Month and Year are now formated as Ints and must be cast as strings.
first_monday_summary['Scrape_Date'] = "FirstMondayAvailable" + first_monday_summary['Month'].astype(str) + "-" + first_monday_summary['Year'].astype(str)


# Stitching Together the Final Summary
pre_pivot = pd.concat([march_10, first_mondays, most_recent, first_monday_summary, first_days], ignore_index=True)
# Remove Potential Duplicates (First Mondays and Most Recent could include duplicates)
pre_pivot = pre_pivot.drop_duplicates(subset=['STATE-COUNTY', 'Scrape_Date'])
final = pre_pivot.pivot(index="STATE-COUNTY", columns="Scrape_Date", values="Population")

#Adding Snapshot Views for Emily 1-17-22
most_recent = df['Scrape_Date'].max()#pd.to_datetime("11-30-2021")
mar_10 = pd.to_datetime("03-10-2020")
may_1 = pd.to_datetime("05-01-2020")

day_length = (most_recent - mar_10).days

sections = 4

q1 = pd.to_datetime((mar_10 + pd.Timedelta(days=day_length/sections)).date())
q2 = pd.to_datetime((mar_10 + pd.Timedelta(days=(day_length/sections*2))).date())
q3 = pd.to_datetime((mar_10 + pd.Timedelta(days=(day_length/sections*3))).date())

dfs = []

dates = [mar_10, q1, q2, q3, most_recent, may_1]
renames = [("Mar 10th Population", "Mar 10th Date"),
          ("Midpoint 1 Population", "Midpoint 1 Date"),
          ("Midpoint 2 Population", "Midpoint 2 Date"),
          ("Midpoint 3 Population", "Midpoint 3 Date"),
          ("Most Recent Population", "Most Recent Date"),
          ("May 1st 2020 Population", "May 1st 2020 Date")]
for n, date in enumerate(dates):
    temp = find_closest_data2date(date, df)
    population_rename , date_rename = renames[n]
    temp = rename_snapshot(population_rename , date_rename , temp)
    dfs.append(temp)


for n, temp in enumerate(dfs):
    temp = temp.reset_index()
    if n ==0 :
        snapshot = temp
    else:
        snapshot = snapshot.merge(temp, how='left')
print(snapshot.shape)
snapshot = snapshot.set_index('STATE-COUNTY')
#End Snapshot views

## Save Transformed Output to Excel

today = datetime.datetime.now().strftime('%m-%d-%Y') #Create string of today's date
max_scrape_date = max([col for col in final.columns if re.match("\d{4}-\d{2}-\d{2}", str(col))])
min_scrape_date = min([col for col in final.columns if re.match("\d{4}-\d{2}-\d{2}", str(col))])

filename = "Jail_Summaries_{}_to_{}_Generated_{}.xlsx".format(min_scrape_date, max_scrape_date, today)
print('Saving to: {}'.format(filename))

with pd.ExcelWriter(filename) as writer1:
    to_save = [(final, 'FirstMondays'),(jail_totals, 'DailyJailTotals'), (snapshot, "SnapshotViews")]
    #final.to_excel(writer1, sheet_name = 'FirstMondays')
    #jail_totals.to_excel(writer1, sheet_name = 'DailyJailTotals')
    #Fix Column Widths
    for df, sheetname in to_save:
        df.to_excel(writer1, sheet_name = sheetname)
        worksheet = writer1.sheets[sheetname]  # pull worksheet object

        #Update Sheet Formatting - ensures columns are expanded to width of longest element rathen than collapsed.
        for idx, col in enumerate(df):  # loop through all columns
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
                )) + 12  # adding a little extra space
            worksheet.set_column(idx, idx, max_len)  # set column width

