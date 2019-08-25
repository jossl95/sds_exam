import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime
import scraping_class
import re
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from Connector import Connector, ratelimit
import time
import os

# Calling the geolocator from geopy
geolocator = Nominatim(user_agent="SDS Student")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# Defining a function which downloads information about longtitude and latitude
def address_parser (address_list):
    street =  []
    locations = []
    latitude = []
    longitude = []
    for addr in address_list:
        a = addr.split(',')
        a = a[0] + ' ' + a[-1][6:]
        street.append(a)
        loc = geocode(a)
        if loc != None:
            lati = loc.latitude
            long = loc.longitude
            stre = loc.address
        else:
            stre, lati, long = None, None, None
        latitude.append(lati)
        longitude.append(long)
        locations.append(stre)
    return street, locations, latitude, longitude


dir = str(os.getcwd())
datafile = dir + "/boliga/data/housing_data_test.csv"
boliga_data = pd.read_csv(datafile)
chunksize = 25
street, locations, latitude, longitude, = [],[],[],[]
for i, chunk in boliga_data.groupby(np.arange(len(boliga_data)) // chunksize):
    results = address_parser(chunk['Address'])
    street.append(results[0])
    locations.append(results[1])
    latitude.append(results[2])
    longitude.append(results[3])
    df = pd.DataFrame(results)
    file_path = dir + "/boliga/data/location_data_" + str(i) + ".csv"
    with open(file_path, mode='w', encoding='UTF-8',
                  errors='strict', buffering=1) as f:
        f.write(df.to_csv())
    df.free_all()

# Transforming into a pandas dataframe
df_location = pd.DataFrame([street, locations, latitude, longitude],
            index = ['Address_street', 'Location_info', 'Latitude',
                     'Longitude']).T

boliga_data = pd.concat([boliga_data, df_location], axis=1, join='inner')
