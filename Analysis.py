# -*- coding: utf-8 -*-
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import datetime as dt
from scipy.stats import pearsonr
import glob
import matplotlib.dates as mdates
from geopy.distance import geodesic
from dateutil.parser import parse
from geopy.geocoders import Nominatim
import time
import statsmodels

dir = str(os.getcwd())
path_full = dir +'/boliga/data/full_data.csv'
path_msta = dir +'/boliga/data/mstation_data.csv'
path_ssta = dir +'/boliga/data/sstation_data.csv'

full_data = pd.read_csv(path_full, index_col=0)
df_mstation = pd.read_csv(path_msta, index_col=0)
df_mstation[['Longitude','Latitude']] = df_mstation[['Longitude','Latitude']].abs()
df_sstation = pd.read_csv(path_ssta, index_col=0)
df_sstation[['Longitude', 'Latitude']] = df_sstation[['Longitude','Latitude']].abs()

##########################
# Descriptive statistics #
################################################################################
# sample identification
#sample should be from befor 2008
full_data = full_data.loc[full_data['Date_of_sale'] <= '2007-12-31']

# deleting outliers
full_data = full_data.loc[full_data['sqm_price']<80000]
full_data = full_data.loc[full_data['Sell_price']<10000000]
print('missingness on address: ',round((full_data.iloc[:, -1].isna().mean())*100,2), "%")
print('missingness on address: ',full_data.iloc[:, -1].isna().sum())
full_data = full_data[full_data['longitude']>12]

full_data = full_data.sort_values('Date_of_sale')
full_data = full_data.reset_index(drop=True)

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
fig1 = plt.title('a: Size of sold real-estate properties', weight='bold')
fig1 = plt.xlabel('Square meters', color='black')
fig1 = plt.ylabel('Observations', color='black')
fig1 = plt.xlim(0,420)
fig1 = plt.ylim(0,3500)
plt.savefig('fig1.png')

# Rooms in sold properties
fig2 = plt.hist(full_data['Rooms'], color = 'green', edgecolor='black', bins=20, linewidth=1.2)
fig2 = plt.title('b: Rooms in sold real-estate properties', weight='bold')
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
plt.figure(figsize = (13, 4))
plt.plot(log_df['datetime'], log_df.delta_t, color='green')
plt.ylabel('Delta t', color='black')
plt.xlabel('Scraping process', color='black')
plt.title('a: The time it took to make the call for data', weight='bold')
plt.savefig('fig4.png')

# Visualization of the response size through the scraping process
plt.figure(figsize = (6.5, 4))
plt.plot(log_df['datetime'], log_df['response_size'], color='green')
plt.ylabel('bytes in the csv files', color='black')
plt.xlabel('Scraping process', color='black')
plt.title('b: The size of csv files through the scraping process', weight='bold')
plt.savefig('fig5.png')

# Plot the delta_t against the response_size - to see correlation.
fig, ax = plt.subplots(figsize = (5, 4))
ax.scatter(log_df.delta_t.astype('float'), log_df.response_size, color = 'green')
ax.fmt_xdata = mdates.DateFormatter('%s-%f')
plt.ylabel('')
plt.xlabel('Delta t', color='black')
plt.title('c: assocation between Delta t and file size', weight='bold')
plt.savefig('fig6.png')

q_cols = log_df.loc[:, ['delta_t', 'response_size']]
print(q_cols.corr(method='pearson'))

#Preparing DataFrame with monthly change
dta = full_data['Date_of_sale']
dta1 = pd.to_datetime(dta)
merged1 = pd.concat([dta1, full_data['sqm_price']], axis=1)
merged1 = merged1.set_index('Date_of_sale')

monthly_price = round(merged1['sqm_price'].resample('M').mean(),2)
monthly_price = monthly_price.to_frame()

monthly_price['Monthly change'] = round(monthly_price.pct_change(periods=1, axis=0),2)
def percent_change(df):
    monthly_price['Total change'] = round((100 * (monthly_price.sqm_price/monthly_price.iloc[0].sqm_price -1)),2)
    return monthly_price

monthly_price.groupby('Date_of_sale').apply(percent_change)
monthly_price.index = pd.to_datetime(monthly_price.index)

#Figure 9
fig, ax = plt.subplots()
ax.plot(monthly_price['Monthly change'], color='green')
plt.title('a: Monthly change in sqare meter price', weight = 'bold')
plt.xlabel('Year', color='black')
plt.ylabel('Change, %', color='black')
fig.autofmt_xdate()
ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
plt.savefig('fig9.png')

# Figure 10
fig, ax = plt.subplots()
ax.plot(monthly_price['Total change'], color ='green')
plt.title('b: Total change in square meter price since 1995', weight='bold')
plt.xlabel('Year', color='black')
plt.ylabel('Change, %', color='black')
fig.autofmt_xdate()
ax.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
plt.savefig('fig10.png')

##########################
# Descriptive statistics #
################################################################################
# sample identification
#sample should be from befor 2008
full_data = full_data.loc[full_data['Date_of_sale'] <= '2007-12-31']

# deleting outliers
full_data = full_data.loc[full_data['sqm_price']<80000]
full_data = full_data.loc[full_data['Sell_price']<10000000]
print('missingness on address: ',round((full_data.iloc[:, -1].isna().mean())*100,2), "%")
full_data = full_data[full_data['longitude']>12]

full_data = full_data.sort_values('Date_of_sale')
full_data = full_data.reset_index(drop=True)
print(full_data.shape)

#############################
# Location based statistics #
################################################################################
#folium map
from folium import plugins
import folium
def map_points(df, lat_col='latitude', lon_col='longitude', zoom_start=11, \
                plot_points=False, pt_radius=2, \
                draw_heatmap=False, heat_map_weights_col=None, \
                heat_map_weights_normalize=True, heat_map_radius=15):
    """Creates a map given a dataframe of points. Can also produce a heatmap overlay

    Arg:
        df: dataframe containing points to maps
        lat_col: Column containing latitude (string)
        lon_col: Column containing longitude (string)
        zoom_start: Integer representing the initial zoom of the map
        plot_points: Add points to map (boolean)
        pt_radius: Size of each point
        draw_heatmap: Add heatmap to map (boolean)
        heat_map_weights_col: Column containing heatmap weights
        heat_map_weights_normalize: Normalize heatmap weights (boolean)
        heat_map_radius: Size of heatmap point

    Returns:
        folium map object
    """

    ## center map in the middle of points center in
    middle_lat = df[lat_col].median()
    middle_lon = df[lon_col].median()

    curr_map = folium.Map(location=[middle_lat, middle_lon],
                          zoom_start=zoom_start,
                          tiles='Stamen Terrain')

    # add points to map
    if plot_points:
        for _, row in df.iterrows():
            folium.CircleMarker([row[lat_col], row[lon_col]],
                                radius=pt_radius,
                                popup=row['location'],
                                fill_color="#3db7e4", # divvy color
                               ).add_to(curr_map)

    # add heatmap
    if draw_heatmap:
        # convert to (n, 2) or (n, 3) matrix format
        if heat_map_weights_col is None:
            cols_to_pull = [lat_col, lon_col]
        else:
            # if we have to normalize
            if heat_map_weights_normalize:
                df[heat_map_weights_col] = \
                    df[heat_map_weights_col] / df[heat_map_weights_col].sum()

            cols_to_pull = [lat_col, lon_col, heat_map_weights_col]

        houses = df[cols_to_pull].values
        curr_map.add_child(plugins.HeatMap(houses, radius=heat_map_radius,gradient={.4: 'green', .85:'yellow', 1: 'red'}))


    return curr_map

# define map
# Creating a map of Copenhagen (specified to the area that we need)
m = folium.Map([55.676098, 12.568337], zoom_start=12)

# Adding s-train stations to the map
for index, row in df_sstation.iterrows():
    folium.Circle(
        radius=50,
        location=(row['Latitude'],row['Longitude']),
        popup=row['Station'],
        fill=True,
        color = 'Blue').add_to(m)

# Adding metro-stations to the map
for index, row in df_mstation.iterrows():
    folium.Circle(
        radius=50,
        location=(row['Latitude'],row['Longitude']),
        popup=row['Station'],
        fill=True,
        color = 'Red').add_to(m)

m

# Adding the heatmap of sold properties to the map with s-train and metro-stations
Sold_houses = full_data[['latitude', 'longitude']].values
m.add_child(plugins.HeatMap(Sold_houses, radius=10))

Train_legend =   '''
                <div style="position: fixed; background:white;
                            top: 150px; right: 350px; width: 170px; height: 75;
                            border:3px solid grey; z-index:9999; font-size:16.5px; font-color=black; weight='bold';
                            ">&nbsp; Station type: <br>
                              &nbsp; Metro-station &nbsp; <i class="fa fa-circle-thin fa-1x" style="color:red"></i><br>
                              &nbsp; S-train station &nbsp; <i class="fa fa-circle-thin fa-1x" style="color:blue"></i>
                </div>
                '''

m.get_root().html.add_child(folium.Element(Train_legend))
m.save('copenhagen_map.html')

address = pd.DataFrame([a[0] + ' ' + a[-1][6:]\
           for a in full_data['Address'].str.split(',')])
header=['ad']
address.columns=header

municipalities = ['Frederiksberg C', 'Frederiksberg', 'København K', 'København V', 'København S', 'København N', 'København Ø']

address_list = []

for i in address['ad']:
    for x in municipalities:
        if x in i:
            address_list.append(x)
            break

# Making a list of municipalities
Municipality = pd.DataFrame(address_list)
Municipality.columns=['Municipality']

# Adding the list to the housing data
full_data['Municipality'] = Municipality

# using selenium to safe HTML as png
from selenium import webdriver
path2gecko = '/Users/MacbookJos/git/geckodriver' # define path  geckodriver
browser = webdriver.Firefox(executable_path=path2gecko)
html= 'file:///Users/MacbookJos/git/sds_exam/copenhagen_map.html'

browser.get(html)
time.sleep(5)
browser.save_screenshot('fig8.png')
browser.quit()

# #parse the opening year from int to string to datetime
# df_sstation['Opened'] = df_sstation['Opened'].apply(lambda x: str(x))
# df_mstation['Opened'] = df_mstation['Opened'].apply(lambda x: str(x))
# df_sstation['Opened'] = df_sstation['Opened'].apply(lambda x: parse(x))
# df_mstation['Opened'] = df_mstation['Opened'].apply(lambda x: parse(x))
# #parse the date of sale from to datetime
# full_data['Date_of_sale'] = full_data['Date_of_sale'].apply(lambda x: str(x))
# full_data['Date_of_sale'] = full_data['Date_of_sale'].apply(lambda x: parse(x))
#
# #calculate which station were open at the year of the sale
# def was_opened(property_, stations):
#     open_stations = {}
#     for i in range(len(stations)):
#         if (stations.iat[i,2] < property_[2]) == True:
#             open_stations[stations.iat[i,0]] = (abs(stations.iat[i,-1]), abs(stations.iat[i,-2]))
#     return open_stations
#
# #calculate distance to closeste open station
# def get_distance_opened(property_, df_stations):
#     stations = was_opened(property_, df_stations)
#     if  stations == {}:
#         return None, None
#     property_loc = (property_[-2], property_[-1])
#     if property_loc[0] is not None or property_loc[1] is not None:
#         min_dist = 999999999999999999999
#         min_dist_station = ''
#         for station, station_loc in stations.items():
#             dist = geodesic(station_loc, property_loc).km
#             if dist < min_dist:
#                 min_dist = dist
#                 min_dist_station = station
#         return round(min_dist,5), min_dist_station
#     else:
#         return None, None
#
# def was_not_opened(property_, stations):
#     open_stations = {}
#     for i in range(len(stations)):
#         if (stations.iat[i,2] > property_[2]) == True:
#             open_stations[stations.iat[i,0]] = (abs(stations.iat[i,-1]),abs(stations.iat[i,-2]))
#     return open_stations
#
#
# def get_distance_not_opened(property_, df_stations):
#     stations = was_not_opened(property_, df_stations)
#     if  stations == {}:
#         return None, None
#     property_loc = (property_[-2], property_[-1])
#     if property_loc[0] is not None or property_loc[1] is not None:
#         min_dist = 999999999999999999999
#         min_dist_station = ''
#         for station, station_loc in stations.items():
#             dist = geodesic(station_loc, property_loc).km
#             if dist < min_dist:
#                 min_dist = dist
#                 min_dist_station = station
#         return round(min_dist,3), min_dist_station
#     else:
#         return None, None
#
#
# geolocator = Nominatim(user_agent="Social Data Science Student", timeout =50)
# copenhagen = geolocator.geocode("Copenhagen")[-1]
#
# def dist_city_center(property_):
#     property_loc = (property_[-2], property_[-1])
#     if property_loc[0] is not None or property_loc[1] is not None:
#         return round(geodesic(property_loc, copenhagen).km, 3)
#     else:
#         return None
#
# # construct distance objects
# distance_m = full_data.apply(lambda x: get_distance_opened(x, df_mstation), axis=1)
# distance_s = full_data.apply(lambda x: get_distance_opened(x, df_sstation), axis=1)
# distance_m_const = full_data.apply(lambda x: get_distance_not_opened(x, df_mstation), axis=1)
# distance_s_const = full_data.apply(lambda x: get_distance_not_opened(x, df_sstation), axis=1)
# distance_c = full_data.apply(dist_city_center, axis=1)
#
# full_data['m_distance'], full_data['m_station'] = [m[0]for m in distance_m], [m[1]for m in distance_m]
# full_data['s_distance'], full_data['s_station'] = [s[0]for s in distance_s], [s[1]for s in distance_s]
# full_data['m_distance_const'], full_data['m_station_const'] = [m[0]for m in distance_m_const], [m[1]for m in distance_m_const]
# full_data['s_distance_const'], full_data['s_station_const'] = [s[0]for s in distance_s_const], [s[1]for s in distance_s_const]
# full_data['c_distance'] = distance_c
#
# # construct percentage differenc from roling mean
# full_data['z_sqm_price'] = (full_data.sqm_price -\
#                             full_data.sqm_price.rolling(window=30).mean())\
#                             / full_data.sqm_price.rolling(window=30).mean()*100
#
# # safe file
# file_path = dir + "/boliga/data/analysis_data.csv"
# with open(file_path, mode='w', encoding='UTF-8',
#               errors='strict', buffering=1) as f:
#     f.write(full_data.to_csv())
path_analysis = dir +'/boliga/data/analysis_data.csv'
full_data = pd.read_csv(path_analysis, index_col=0)

# ensuring date time as column type
full_data['Date_of_sale'] = full_data['Date_of_sale'].apply(lambda x: str(x))
full_data['Date_of_sale'] = full_data['Date_of_sale'].apply(lambda x: parse(x))

# make distance plot
from skmisc.loess import loess
from scipy.signal import savgol_filter

datafile = dir + "/boliga/data/analysis_data.csv"
df = pd.read_csv(datafile, index_col=0)
df = df[['z_sqm_price', 'm_distance']]
df = df.dropna()
df.sort_values('z_sqm_price')

x = df['m_distance'].to_numpy()
y = df['z_sqm_price'].to_numpy()
sorted_indices = np.argsort(x)
sorted_x = x[sorted_indices]

l = loess(x, y, frac=0.1)
l.fit()
pred = l.predict(sorted_x, stderror=True)
conf = pred.confidence()

lowess = pred.values
ll = conf.lower
ul = conf.upper

# plt.plot(x, y, '+')
fig, ax1 = plt.subplots( figsize=(13, 4))
ax1.plot(sorted_x, lowess, color='green')
ax1.fill_between(sorted_x,ll,ul,alpha=.15, color='green')
ax1.set_ylabel('%-difference in square meter price from rolling mean')
ax1.set_xlabel("distance in kilometers")
ax1.set_xlim(0,5)
plt.savefig('fig7.png')

# make year of opening plot_point
# defining which stations opened when
m2002 = ['Sundby', 'Ørestad', 'Nørreport', 'Lergravsparken', 'Kongens Nytorv', 'Islands Brygge', 'DR Byen', 'Bella Center', 'Amagerbro']
m2003 = ['Fasanvej', 'Forum', 'Frederiksberg', 'Lindevang']
m2004 = ['Flintholm']
m2007 = ['Amager Strand', 'Øresund','Femøren', 'Kastrup']

open2002 = full_data.loc[full_data['m_station'].isin(m2002)]
open2003 = full_data.loc[full_data['m_station'].isin(m2003)]
open2004 = full_data.loc[full_data['m_station'].isin(m2004)]
open2007 = full_data.loc[full_data['m_station'].isin(m2007)]

open2002_c = full_data.loc[full_data['m_station_const'].isin(m2002)]
open2003_c = full_data.loc[full_data['m_station_const'].isin(m2003)]
open2004_c = full_data.loc[full_data['m_station_const'].isin(m2004)]
open2007_c = full_data.loc[full_data['m_station_const'].isin(m2007)]

distance_to_metro2002 = open2002['m_distance']
distance_to_metro2003 = open2003['m_distance']
distance_to_metro2004 = open2004['m_distance']
distance_to_metro2007 = open2007['m_distance']

open2002.insert(0, 'distance_to_metro', distance_to_metro2002)
open2003.insert(0, 'distance_to_metro', distance_to_metro2003)
open2004.insert(0, 'distance_to_metro', distance_to_metro2004)
open2007.insert(0, 'distance_to_metro', distance_to_metro2007)

distance_to_metro2002 = open2002_c['m_distance_const']
distance_to_metro2003 = open2003_c['m_distance_const']
distance_to_metro2004 = open2004_c['m_distance_const']
distance_to_metro2007 = open2007_c['m_distance_const']

open2002_c.insert(0, 'distance_to_metro', distance_to_metro2002)
open2003_c.insert(0, 'distance_to_metro', distance_to_metro2003)
open2004_c.insert(0, 'distance_to_metro', distance_to_metro2004)
open2007_c.insert(0, 'distance_to_metro', distance_to_metro2007)

open2002 = open2002.append(open2002_c, ignore_index=True)
open2003 = open2003.append(open2003_c, ignore_index=True)
open2004 = open2004.append(open2004_c, ignore_index=True)
open2007 = open2007.append(open2007_c, ignore_index=True)

def group_dist(dist):
    if dist < 0.5:
        x = 'Less than 0.5 km'
    elif dist >= 0.5 and dist <= 1:
        x = '0.5 -1 km'
    else:
        x = 'Above 1 km'
    return x

dist_group2002 = open2002['distance_to_metro'].apply(lambda x: group_dist(x))
dist_group2003 = open2003['distance_to_metro'].apply(lambda x: group_dist(x))
dist_group2004 = open2004['distance_to_metro'].apply(lambda x: group_dist(x))
dist_group2007 = open2007['distance_to_metro'].apply(lambda x: group_dist(x))
open2002.insert(1, 'Grouped_distance', dist_group2002)
open2003.insert(1, 'Grouped_distance', dist_group2003)
open2004.insert(1, 'Grouped_distance', dist_group2004)
open2007.insert(1, 'Grouped_distance', dist_group2007)

open2002['Date_of_sale'] = open2002['Date_of_sale'].apply(lambda x: x.year)
open2003['Date_of_sale'] = open2003['Date_of_sale'].apply(lambda x: x.year)
open2004['Date_of_sale'] = open2004['Date_of_sale'].apply(lambda x: x.year)
open2007['Date_of_sale'] = open2007['Date_of_sale'].apply(lambda x: x.year)

fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, figsize=(13, 20))
# fig.suptitle('%-difference in square meter price time from opening date', weight="bold")
plt.tight_layout(pad=3, w_pad=2.5, h_pad=2)

sns.lineplot(data=open2002, x='Date_of_sale', y='z_sqm_price', hue='Grouped_distance', palette='Greens', ax=ax1)
ax1.axvline(2002, color='k', linestyle=':')
ax1.set_xlabel("Year which property was sold")
ax1.set_ylabel("%-difference in square meter price from rolling mean")
ax1.margins(0)
sns.lineplot(data=open2003, x='Date_of_sale', y='z_sqm_price', hue='Grouped_distance', palette='Greens', ax=ax2)
ax3.axvline(2004, color='k', linestyle=':')
ax3.set_xlabel("Year which property was sold")
ax3.set_ylabel("%-difference in square meter price from rolling mean")
ax3.margins(0)
sns.lineplot(data=open2004, x='Date_of_sale', y='z_sqm_price', hue='Grouped_distance', palette='Greens', ax=ax3)
ax2.axvline(2003, color='k', linestyle=':')
ax2.set_xlabel("Year which property was sold")
ax2.set_ylabel("%-difference in square meter price from rolling mean")
ax2.margins(0)
sns.lineplot(data=open2007, x='Date_of_sale', y='z_sqm_price', hue='Grouped_distance', palette='Greens', ax=ax4)
ax4.axvline(2007, color='k', linestyle=':')
ax4.set_xlim(1998,2008)
ax4.set_xlabel("Year which property was sold")
ax4.set_ylabel("%-difference in square meter price from rolling mean")
ax4.legend(loc=1)
ax4.margins(0)
plt.savefig('fig11.png')


fig, ax = plt.subplots(figsize = (13, 4))
sns.lineplot(data=open2002, x='Date_of_sale', y='z_sqm_price', hue='Grouped_distance', palette='Greens', ax=ax)
# plt.title('%-difference in square meter price time from opening date', weight="bold")
ax.axvline(2002, color='k', linestyle=':')
ax.legend(loc=4)
ax.set_xlabel("Year which property was sold")
ax.set_ylabel("%-difference in square meter price from rolling mean")
plt.ylim(-30,20)
plt.margins(0)
plt.savefig('fig11_2002.png')
