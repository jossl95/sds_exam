import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import datetime as dt
from scipy.stats import pearsonr
import glob

dir = str(os.getcwd())
path_full = dir +'/boliga/data/full_data.csv'
path_msta = dir +'/boliga/data/mstation_data.csv'
path_ssta = dir +'/boliga/data/sstation_data.csv'

full_data = pd.read_csv(path_full)
df_mstation = pd.read_csv(path_msta)
df_sstation = pd.read_csv(path_ssta)

################
# Data quality #
################################################################################
# Random sample of the data scraped from boliga.dk
print("random sample of final data, \n",full_data.sample(100))

# Random sample of data from the metro stations
print("m-train station, \n", df_mstation)

# Random sample of data from the s-train stations
print("s-train station, \n", df_sstation)

# Checking for duplicates
print("checking for duplicates: ",len(full_data), len(full_data.drop_duplicates()))

# Shows the amount of different real estate types (appartments, houses, terraced house)
print("count py real estate types: ", full_data['Type'].value_counts())

# Descriptive statistics
totalsum = np.sum(full_data['Type'].value_counts())
ListOfTypes = ['Ejerlejlighed', 'Villa', 'Rækkehus','Fritidshus']
for i in ListOfTypes:
    cd = full_data['Type'].value_counts()
    for i in cd:
        percent_type = round(cd/totalsum ,3)

percent_df = pd.DataFrame(percent_type)
# Rename columns
percent_df = percent_df.rename(columns={'Type':'Share of total'})
print("descriptive statistics: \n", percent_df)

# Histogram of the distrubution of square meters
fig1 = plt.hist(full_data['m2'], color = 'green', edgecolor = 'black', bins=40, linewidth=1.2)
fig1 = plt.title('Size of sold real-estate properties', weight='bold')
fig1 = plt.xlabel('Square meters', color='black')
fig1 = plt.ylabel('Observations', color='black')
fig1 = plt.xlim(0,420)
fig1 = plt.ylim(0,3500)
plt.savefig('fig1.png')

# Rooms in sold properties
fig2 = plt.hist(full_data['Rooms'], color = 'green', edgecolor='black', bins=20, linewidth=1.2)
fig2 = plt.title('Rooms in sold real-estate properties', weight='bold')
fig2 = plt.xlabel('Rooms', color='black')
fig2 = plt.ylabel('Observations', color='black')
fig2 = plt.xlim(0,12)
fig2 = plt.ylim(0,5000)
plt.savefig('fig2.png')

# Building year of sold properties
fig3 = plt.hist(full_data['Building_year'], bins=90, color= 'green', edgecolor='black', linewidth = 1.2)
fig3 = plt.title('Building year of real-estate properties', weight='bold')
fig3 = plt.xlabel('Year', color='black')
fig3 = plt.ylabel('Observations', color='black')
fig3 = plt.xlim(1600, 2030)
fig3 = plt.ylim(0, 3500)
plt.savefig('fig3.png')


# Loading the logfile as a pandas dataframe
log_df = pd.read_csv('log_boliga.csv', sep=';')
log_df = log_df.iloc[:,:-2]
log_df = log_df.drop(['response_size'], axis=1)
log_df['dt'] = pd.to_datetime(log_df['t'], unit= 's')
log_df.sort_values('id')
log_df.columns = ['id', 'project', 'connector_type', 't', 'delta_t', 'url',
                  'response_size', 'response_code', 'datetime' ]
print("head of log_file: \n", log_df.head())

# Visualization of the time it took to make the call for data
plt.style.use('ggplot')
plt.figure(figsize = (12, 4))
plt.plot(log_df['datetime'], log_df.delta_t, color='green')
plt.ylabel('Delta t', color='black')
plt.xlabel('Scraping process', color='black')
plt.title('The time it took to make the call for data', weight='bold')
plt.savefig('fig4.png')

# Visualization of the response size through the scraping process
plt.style.use('ggplot')
plt.figure(figsize = (12, 4))
plt.plot(log_df['datetime'], log_df['response_size'], color='green')
plt.ylabel('bytes in the csv files', color='black')
plt.xlabel('Scraping process', color='black')
plt.title('The size of csv files through the scraping process', weight='bold')
plt.savefig('fig5.png')

# Plot the delta_t against the response_size - to see correlation.
q_cols = log_df.loc[:, ['delta_t', 'size']]
print(q_cols.corr(method='pearson'))
