import requests
import pandas as pd
import matplotlib.pyplot as plt
import datetime


df = pd.read_csv('https://raw.githubusercontent.com/publicsafetylab/public-psl-jdi-pops/master/data.csv')


orig_len = len(df)


#Reformat Date Column to be Timestamp
df['Scrape_Date'] = pd.to_datetime(df['Scrape_Date'])
df = df[df['Scrape_Date'] >= pd.to_datetime("03-10-2020")]
after_mar10 = len(df)


print("{} post march 10th.".format(after_mar10/orig_len))


# ## Subset to Jails with >75% days populated with data



ndata_per_jail = df.groupby('Facility_Identifier')['Population'].count()
max_ex = max(ndata_per_jail)
threshold = .75*max_ex
jails_to_include = ndata_per_jail[ndata_per_jail>=threshold].index

df = df[df['Facility_Identifier'].isin(jails_to_include)]
over_75percent_data = len(df)



# ### Quantifying Data Drop-Off


percent = str(round(len(jails_to_include)/len(ndata_per_jail),2)*100)
message = "{} of {} jails ({}%) have over 75% data populated post march 10th."
print(message.format(len(jails_to_include),len(ndata_per_jail), percent))


# ## Jails with the last date needs to be within 3 months

# In[12]:


today = pd.to_datetime(datetime.datetime.now().strftime('%m-%d-%Y')) 
three_months_ago = today - datetime.timedelta(days = 90)


# In[13]:


jails_most_recent = df.groupby('Facility_Identifier')['Scrape_Date'].max()
jails_to_include = jails_most_recent[jails_most_recent>=three_months_ago].index
df = df[df['Facility_Identifier'].isin(jails_to_include)] #Subset master dataset


# ## Quantifying Drop Off Again: Generally have seen that all jails that have 75% of data populated also include data from within 3 months:



within_3_months = len(df)
within_3_months / over_75percent_data

## Create Month / Day of Week / Year Columns 
df['Month'] = df.Scrape_Date.dt.strftime('%m')
df['Year'] = df.Scrape_Date.dt.strftime('%Y')
df['DayOfWeek'] = df.Scrape_Date.dt.strftime('%A')

# ## Summarizing Daily Jail Populations


jail_totals = df.groupby('Scrape_Date')['Population'].agg(['count', 'sum'])


jail_totals.columns = ['Count_Of_Jails','Total_Jail_Population']
jail_totals['Seven_Day_Rolling_Average'] = jail_totals['Total_Jail_Population'].rolling(7).mean()


# ## TBD: programatically create list of dates based on most recent weekday (or potentially specific day of week) for each month

# ## First Day of Each Month

# In[64]:


first_days = df.groupby(['Facility_Identifier', 'Month', 'Year']).head(1)
first_days['Scrape_Date'] = "FirstDay" + first_days['Month'] + "-" + first_days['Year']


# ## First Monday of Each Month

# In[19]:

# In[57]:



mondays = df[df.DayOfWeek=='Monday']
first_mondays = mondays.groupby(['Facility_Identifier', 'Month', 'Year']).head(1)


# In[58]:


most_recent = df.groupby(['Facility_Identifier']).tail(1)

# In[27]:


## Verify DataFrame Correctly Sorted by Facility/Date
assert(all(df == df.sort_values(by=['Facility_Identifier', 'Scrape_Date'])))
## Ensure there is one per Facility
assert(len(most_recent) == df.Facility_Identifier.nunique())


# ## March 10th

# In[59]:


march_10 = df[df.Scrape_Date == pd.to_datetime("03-10-2020")]

# ## First Mondays Summary

# In[68]:


first_monday_summary = first_mondays.groupby(['Facility_Identifier', 'Month', 'Year']).head(1)
first_monday_summary['Scrape_Date'] = "FirstMonday" + first_monday_summary['Month'] + "-" + first_monday_summary['Year']

# Stitching Together the Final Summary


pre_pivot = pd.concat([march_10, first_mondays, most_recent, first_monday_summary, first_days], ignore_index=True)

final = pre_pivot.pivot(index="Facility_Identifier", columns="Scrape_Date", values="Population")






# ## Save Transformed Output to Excel

# In[73]:


today = datetime.datetime.now().strftime('%m-%d-%Y') #Create string of today's date

filename = "Jail_Summaries_as_of_{}.xlsx".format(today)
print('Saving to: {}'.format(filename))

with pd.ExcelWriter(filename) as writer1:
    final.to_excel(writer1, sheet_name = 'FirstMondays')
    jail_totals.to_excel(writer1, sheet_name = 'DailyJailTotals')

