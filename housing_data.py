##############################
#                            #
#    Housing Data Scraper    #
#                            #
##############################

#Defining function that gets longtitude and latitude
from geopy.geocoders import Nominatim
from Connector import Connector, ratelimit
import pandas as pd
import time
import os

logfile = 'Scraping_file.csv'## name your log file.
connector = Connector(logfile)

def scraping_function(url):
    response, call_id = connector.get(url,'SDS_exam scrape')

    #checks if our response fucntion is working properly
    if response.ok:
        html = response.text
    else:
        print('error')

    #Turning the response into a Pandas dataframe
    df_table = pd.read_html(response.text)[0]

    #Fixes problem whith column name in each row (replace column name with whitespace)
    for column in list(df_table.columns):
        df_table[column] = df_table[column].str.replace(column, '')

    #Tanslating the columns to english, and delete two columns that we don't intend to use
    df_table.columns = ['Address', "Sell_price", "Date_of_sale", "Type",
                        "sqm_price", "Rooms", "m2","Building_year", "PD", "UN"]
    df_table = df_table.drop(['PD', 'UN'], axis = 1 )

    #fixing dublicates of the type of residence
    for i in range(len(df_table['Type'])):
        for j in ['EEjerlejlighed', 'VVilla', 'RRækkehus']:
            df_table['Type'][i] = df_table['Type'][i].replace(j,'')

    #Changing the prices, so that it's numeric values
    for s in range(len(df_table['Sell_price'])):
        df_table['Sell_price'][s] = df_table['Sell_price'][s].replace('.','')
        df_table['sqm_price'][s] = df_table['sqm_price'][s].replace('.','')

    df_table['sqm_price'] = pd.to_numeric(df_table['sqm_price'])
    df_table['Sell_price'] = pd.to_numeric(df_table['Sell_price'])

    #Changing the date into date-time format
    def format_dates(date):
        q = date.split('-')
        return q[1].strip() + q[0].strip() + q[2]

    df_table['Date_of_sale'] = df_table['Date_of_sale'].apply(lambda x: format_dates(x))
    df_table['Date_of_sale'] = pd.to_datetime(df_table['Date_of_sale'], format='%m%d%Y')

    #calling the geolocator from geopy
    geolocator = Nominatim(user_agent="Social Data Science Student", timeout = 20)

    #Defining functions which downloads information about longtitude and latitude
    def address_parser (address_list):
        street =  []
        locations = []
        latitude = []
        longitude = []
        for addr in address_list:
            a = addr.split(',')
            a = a[0] + ' ' + a[-1][6:]
            street.append(a)
            loc = geolocator.geocode(a)
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

    street, locations, latitude, longitude = address_parser(df_table['Address'])

    #Transforming into a pandas dataframe
    df_location = pd.DataFrame([street, locations, latitude, longitude],
                index = ['Address', 'Location_info', 'Latitude', 'Longitude']).T

    df = pd.concat([df_table, df_location], axis=1, join='inner')
    dir = str(os.getcwd())
    file_path = str(dir + '/boliga' +'/scrape' +'/%d'%call_id)

    with open(file_path, mode='w', encoding='UTF-8',
              errors='strict', buffering=1) as f:
        f.write(df.to_csv())

    return df

dir = str(os.getcwd())
project = 'boliga'
## create project folder
if not os.path.isdir(project):    # check if folder exist
    os.mkdir(project)

price_data = []
max_page = 5 #288
for p in range(max_page):
    url = 'https://www.boliga.dk/salg/resultater?salesDateMin=1995&zipcodeFrom=1000&zipcodeTo=2499&searchTab=1&page='\
    + str(p) + '&sort=date-a'
    price_data.append(scraping_function(url))
    time.sleep(1)

p = pd.concat(price_data, axis=0)
p = p.reset_index()

file_path = dir + "/data" + "/test_scrape"
with open(file_path, mode='w', encoding='UTF-8',
              errors='strict', buffering=1) as f:
    f.write(p.to_csv())
