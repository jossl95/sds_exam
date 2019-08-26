
# -*- coding: utf-8 -*-
from selenium import webdriver
import time, os, glob, re
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

#######################
#    Scraping data    #
################################################################################

#Initialization of selenium
path2gecko = '/Users/MacbookJos/git/geckodriver' # define path  geckodriver
browser = webdriver.Firefox(executable_path=path2gecko)

# direction to boliga.dk
browser.get('https://www.boliga.dk/salg/resultater?salesDateMin=1995&zipcodeFrom=1000&zipcodeTo=2499&searchTab=1&page=1&sort=date-a')
cookie_button = browser.find_element_by_xpath('//*[@id="coiAccept"]')
cookie_button.click()

def df2table(html):
    df = pd.read_html(html)[0]
    df = df.iloc[:, 0:8]
    for column in list(df.columns):
        df[column] = df[column].str.replace(column, '')
    df.columns = ["Address", "Sell_price", "Date_of_sale", "Type",
                  "sqm_price", "Rooms", "m2", "Building_year"]

    #  Removing dublicates of the type of residence
    for i in range(len(df['Type'])):
        for j in ['EEjerlejlighed', 'VVilla', 'RRækkehus', 'FFritidshus']:
            df['Type'][i] = df['Type'][i].replace(j,'')

    # Changing the prices, so that it's numeric values
    for s in range(len(df['Sell_price'])):
        df['Sell_price'][s] = df['Sell_price'][s].replace('.','')
        df['sqm_price'][s] = df['sqm_price'][s].replace('.','')

    df['sqm_price'] = pd.to_numeric(df['sqm_price'])
    df['Sell_price'] = pd.to_numeric(df['Sell_price'])
    #Changing the date into date-time format
    def format_dates(date):
        q = date.split('-')
        return q[0].strip() + q[1].strip() + q[2]

    df['Date_of_sale'] = df['Date_of_sale'].apply(lambda x: format_dates(x))
    df['Date_of_sale'] = pd.to_datetime(df['Date_of_sale'], format='%d%m%Y')

    return df

i = 1
dir = str(os.getcwd())
while i <= 288: #288
    ## Initialization of log
    logfile = 'log_boliga_csv'
    project_name = 'SDS exam'
    header = ['id','project','connector_type','t', 'delta_t', 'url',\
              'redirect_url','response_size', 'response_code','success', 'error']

    if os.path.isfile(logfile):
        log = open(logfile,'a')
    else:
        log = open(logfile,'w')
        log.write(';'.join(header))

    ## load log
    with open(logfile,'r') as f: # open file
        l = f.read().split('\n') # read and split file by newlines.
        ## set id
        if len(l)<=1:
            id = 0
        else:
            id = int(l[-1][0])+1

    t = time.time()

    """
    NOTE: several indicators are different than from the indicators
    that are established with the requests package. `dt`, does not
    necessarily reflect the complete load time. `size` might not be
    correct as selenium works in the background and could still be
    loading
    """
    html = browser.page_source
    df = df2table(html)

    ## Key arguments
    err = ''                           # define python error variable as empty assumming success.
    success = True                     # define success variable
    connector_type = "selenium"
    redirect_url = browser.current_url # log current url, after potential redirects
    dt = t - time.time()               # define delta-time waiting for the server and downloading content.
    size = len(browser.page_source)    # define variable for size of html content of the response.
    response_code = ''                 # log status code.

    ## log...
    call_id = id                        # get current unique identifier for the call

    # Defines row to be written in the log
    row = [call_id, project_name, connector_type, t, dt,\
           redirect_url, size, response_code, success, err] # define row

    log.write('\n'+';'.join(map(str,row))) # write row to log file.
    log.flush()

    # storing parsed table as csv
    file_path = str(dir + '/boliga' +'/scrape' +'/%d.csv'%i)
    with open(file_path, mode='w', encoding='UTF-8',
              errors='strict', buffering=1) as f:
        f.write(df.to_csv())
    time.sleep(2)

    xpath_next = '/html/body/app-root/app-sold-properties-list/div[2]/app-pagination/div[1]/div[4]/a'
    next_button = browser.find_element_by_xpath(xpath_next)
    next_button.click()
    time.sleep(1)
    i += 1

########################
# Apppending csv-files #
########################

#Path to file directory
boliga_directory = dir + '/boliga/scrape/'

#import all the files in the folder
boliga_files = glob.glob(os.path.join(boliga_directory, '*.csv'))

#loop throug all files and read as pandas
merged_df = [] #saves as list of dataframes
for boliga_file in boliga_files:
    df = pd.read_csv(boliga_file)
    merged_df.append(df)

#merge the dataframes with concat
boliga_data = pd.concat(merged_df, ignore_index=True)

#save as csv
file_path = str(dir + '/boliga/data/housing_data.csv')
with open(file_path, mode='w', encoding='UTF-8',
          errors='strict', buffering=1) as f:
    f.write(boliga_data.to_csv())

#########################################
# Scraping from wiki for train stations #
################################################################################

##################
# Metro stations #
##################
logfile = 'log_station_scrape.csv'## name your log file.
connector = Connector(logfile)

# Scrape initial table of stations
url = "https://en.wikipedia.org/wiki/List_of_Copenhagen_Metro_stations"
resp, callid = connector.get(url, 'mstation_scrape')
html = resp.text

# parse table
table = pd.read_html(resp.text)[1]
table = table.drop(['Transfer', 'Line'], axis = 1)
table['Station'] = table['Station']

# Importing meta-data on stations
def links (html):
    s = BeautifulSoup(html, 'html.parser')
    table_html = s.find_all('table')[1]
    urlFmt= re.compile('href="(\S*)"')
    link_locations = urlFmt.findall(str(table_html))
    links = ['https://en.wikipedia.org' + i for i in link_locations]
    links = [link for link in links if '/wiki/' in link]
    for i in ['S-train', 'M3_', 'M4_', 'M2_', 'M1_', 'S-train', 'Template',
              'K%C3%B8ge_Nord','File:', 'List', 'commons.', '_Metro', '_Line',
              'Airport', 'Lokaltog', 'Letbane']:
        links = [link for link in links if not i in link]
    return sorted(list(set(links)))

def mstation_location(urls):
    stations = []
    longitudes = []
    latitudes = []
    for station in urls:
        html = requests.get(station).text
        s = BeautifulSoup(html, 'lxml')
        loc = s.select('span.geo-dms')[0]
        latFmt = re.compile('.*latitude">(.*)</span> <')
        lonFmt = re.compile('.*longitude">(.*)</span><')
        longitudes.append(lonFmt.findall(str(loc)))
        latitudes.append(latFmt.findall(str(loc)))
        stations.append(str(s.title.string).replace(' Station - Wikipedia', ''))
    return stations, longitudes, latitudes

mstation, longitude, latitude = mstation_location(links(html))

# Parsing location data in degrees with decimals
def dms2dd(degrees, minutes, seconds, direction):
    dd = float(degrees) + float(minutes)/60 + float(seconds)/(60*60);
    if direction == 'E' or direction == 'N':
        dd *= -1
    return dd;

def dd2dms(deg):
    d = int(deg)
    md = abs(deg - d) * 60
    m = int(md)
    sd = (md - m) * 60
    return [d, m, sd]

def parse_dms(dms):
    parts = re.split('[°′″]+', str(dms))
    lat = dms2dd(parts[0], parts[1], parts[2], parts[3])
    return (lat)

# Defining the coordinates for the location of the stations
latitude = [parse_dms(lat[0]) for lat in latitude]
longitude = [parse_dms(long[0]) for long in longitude]

# Convert and merge into a pandas dataframe
df_location = pd.DataFrame([mstation, longitude, latitude]).T
df_location.columns = ['Station', 'Longitude', 'Latitude']
df_mstation = pd.merge(table, df_location, on='Station')

# store the metro stations
file_path = dir + "/boliga/data/mstation_data.csv"
with open(file_path, mode='w', encoding='UTF-8',
              errors='strict', buffering=1) as f:
    f.write(df_mstation.to_csv())

####################
# S-train stations #
###############################################################################
# Scrape initial table of stations
url = "https://en.wikipedia.org/wiki/List_of_Copenhagen_S-train_stations"
resp, callid = connector.get(url, 'sstation_scrape')
html = resp.text

# Parse table
table = pd.read_html(resp.text)[1]
table = table.drop(['Transfer', 'Line'], axis = 1)
table['Station'] = table['Station'].str.translate({ord('#'): '', ord('†'): ''})

def sstation_location(urls):
    stations = []
    longitudes = []
    latitudes = []
    for station in urls:
        html = requests.get(station).text
        s = BeautifulSoup(html, 'lxml')
        loc = s.select('span.geo-dms')[0]
        latFmt = re.compile('.*latitude">(.*)</span> <')
        lonFmt = re.compile('.*longitude">(.*)</span><')
        station = str(s.title.string).replace(' Station - Wikipedia', '')
        station = station.replace(' station - Wikipedia', '')
        #appending to containers
        longitudes.append(lonFmt.findall(str(loc)))
        latitudes.append(latFmt.findall(str(loc)))
        stations.append(station)
    return stations, longitudes, latitudes

sstation, longitude, latitude = sstation_location(links(html))

# Defining the coordinates for the location of the stations
latitude = [parse_dms(lat[0]) for lat in latitude]
longitude = [parse_dms(long[0]) for long in longitude]

# Convert and merge into a pandas dataframe
df_location = pd.DataFrame([sstation, longitude, latitude]).T
df_location.columns = ['Station', 'Longitude', 'Latitude']
df_sstation = pd.merge(table, df_location, on='Station')

# Shows the s-train stations
file_path = dir + "/boliga/data/sstation_data.csv"
with open(file_path, mode='w', encoding='UTF-8',
              errors='strict', buffering=1) as f:
    f.write(df_sstation.to_csv())

###################
# Data inspection #
################################################################################
########################################
# Random sample inspection of our data #
########################################
# Random sample of the data scraped from boliga.dk
print(boliga_data.sample(15))

# Random sample of data from the metro stations
print(df_mstation.sample(5))

# Random sample of data from the s-train stations
print(df_sstation.sample(5))

# Checks if our scraped data contains duplicates
# Boliga constains 14.400 sold houses in the chosen period of time (01/01/1995 - 31/12/2007)
print(len(boliga_data), len(boliga_data.drop_duplicates()))

# Shows the amount of different real estate types (appartments, houses, terraced house)
print(boliga_data['Type'].value_counts())
