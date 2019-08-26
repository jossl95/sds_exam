# -*- coding: utf-8 -*-
from selenium import webdriver
import time, os, re
import glob
import pandas as pd
from Connector import Connector, ratelimit
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

# Calling the geolocator from geopy
geolocator = Nominatim(user_agent="SDS Student")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1,
                      max_retries=3, error_wait_seconds=5,
                      return_value_on_exception=True)

# Defining a function which downloads information about longtitude and latitude
def address_parser (address_list):
    street =  []
    locations = []
    latitude = []
    longitude = []
    for addr in address_list:
        loc = geocode(addr)
        time.sleep(.2)
        if loc != None:
            lati = loc.latitude
            time.sleep(.2)
            long = loc.longitude
            time.sleep(.2)
            stre = loc.address
            time.sleep(.2)
        else:
            stre, lati, long = None, None, None
        latitude.append(lati)
        longitude.append(long)
        locations.append(stre)
    return address_list, locations, latitude, longitude

dir = str(os.getcwd())
datafile = dir + "/boliga/data/housing_data.csv"
boliga_data = pd.read_csv(datafile)
# cleaning DataFrame
boliga_data['Date_of_sale'] = pd.to_datetime(boliga_data['Date_of_sale'])
boliga_data.sort_values(by=['Date_of_sale'], inplace=True, ascending=True)
boliga_data = boliga_data.reset_index()
boliga_data = boliga_data.iloc[:, 3:]
df_address = pd.DataFrame([a[0] + ' ' + a[-1][6:]\
                for a in boliga_data['Address'].str.split(',')])
df_address.drop_duplicates(inplace = True)
df_address[0] = df_address[0].str.lstrip()

#check if folder exists
project = dir + '/boliga/location'
if not os.path.isdir(project):
    os.mkdir(project)

print(time.time())
chunksize = 100
dt = []
for i, chunk in df_address.groupby(np.arange(len(df_address)) // chunksize):
    t = time.time()
    building_address, locations, latitude, longitude = address_parser(chunk[0])
    dt.append(t - time.time())
    dict = {'building_address':building_address,'location':locations, 'latitude':latitude, 'longitude': longitude}
    df = pd.DataFrame(dict)
    file_path = dir + "/boliga/location/location_data_" + str(i) + ".csv"
    with open(file_path, mode='w', encoding='UTF-8',
                  errors='strict', buffering=1) as f:
        f.write(df.to_csv())
print(time.time())

file_path = dir + "/boliga/location/location_data_log"
with open(file_path, mode='w', encoding='UTF-8',
              errors='strict', buffering=1) as f:
    f.write(dt)
Transforming into a pandas dataframe
#Path to file directory
location_dir = dir + '/boliga/location/'

#import all the files in the folder
location_files = glob.glob(os.path.join(location_dir, '*.csv'))

#loop throug all files and read as pandas
merged_df = [] #saves as list of dataframes
for file in location_files:
    df = pd.read_csv(file)
    merged_df.append(df)

#merge the dataframes with concat
location_data = pd.concat(merged_df, ignore_index=True)

#save as csv
file_path = str(dir + '/boliga/data/location_data.csv')
with open(file_path, mode='w', encoding='UTF-8',
          errors='strict', buffering=1) as f:
    f.write(location_data.to_csv())
############################
# merge with full datafile #
################################################################################
# load boliga_data
dir = str(os.getcwd())
datafile = dir + "/boliga/data/housing_data.csv"
boliga_data = pd.read_csv(datafile)
boliga_data = boliga_data.iloc[:, 2:]
boliga_data['building_address'] = pd.DataFrame([a[0] + ' ' + a[-1][6:]\
                for a in boliga_data['Address'].str.split(',')])
boliga_data['building_address'] = boliga_data['building_address'].str.lstrip()

# load appended location data
location_file = dir +"/boliga/data/location_data.csv"
location_data = pd.read_csv(location_file)
location_data = location_data.iloc[:,2:]

#merge data

full_data = pd.merge(boliga_data, location_data, on= 'building_address')
full_data = full_data.reset_index()
full_data = full_data.iloc[:,2:]
print(full_data.head())

file_path = str(dir + '/boliga/data/full_data.csv')
with open(file_path, mode='w', encoding='UTF-8',
          errors='strict', buffering=1) as f:
    f.write(full_data.to_csv())
