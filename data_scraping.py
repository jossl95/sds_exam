######################
#      Connector     #
######################
import requests, os, time

def ratelimit():
    "A function that handles the rate of your calls."
    time.sleep(.5) # sleep one second.

class Connector():
  def __init__(self,logfile,overwrite_log=False,connector_type='requests',session=False,path2selenium='',n_tries = 5,timeout=30):
    """This Class implements a method for reliable connection to the internet and monitoring.
    It handles simple errors due to connection problems, and logs a range of information for basic quality assessments

    Keyword arguments:
    logfile -- path to the logfile
    overwrite_log -- bool, defining if logfile should be cleared (rarely the case).
    connector_type -- use the 'requests' module or the 'selenium'. Will have different since the selenium webdriver does not have a similar response object when using the get method, and monitoring the behavior cannot be automated in the same way.
    session -- requests.session object. For defining custom headers and proxies.
    path2selenium -- str, sets the path to the geckodriver needed when using selenium.
    n_tries -- int, defines the number of retries the *get* method will try to avoid random connection errors.
    timeout -- int, seconds the get request will wait for the server to respond, again to avoid connection errors.
    """

    ## Initialization function defining parameters.
    self.n_tries = n_tries # For avoiding triviel error e.g. connection errors, this defines how many times it will retry.
    self.timeout = timeout # Defining the maximum time to wait for a server to response.
    ## not implemented here, if you use selenium.
    if connector_type=='selenium':
      assert path2selenium!='', "You need to specify the path to you geckodriver if you want to use Selenium"
      from selenium import webdriver
      ## HIN download the latest geckodriver here: https://github.com/mozilla/geckodriver/releases

      assert os.path.isfile(path2selenium),'You need to insert a valid path2selenium the path to your geckodriver. You can download the latest geckodriver here: https://github.com/mozilla/geckodriver/releases'
      self.browser = webdriver.Firefox(executable_path=path2selenium) # start the browser with a path to the geckodriver.

    self.connector_type = connector_type # set the connector_type

    if session: # set the custom session
      self.session = session
    else:
      self.session = requests.session()
    self.logfilename = logfile # set the logfile path
    ## define header for the logfile
    header = ['id','project','connector_type','t', 'delta_t', 'url', 'redirect_url','response_size', 'response_code','success','error']
    if os.path.isfile(logfile):
      if overwrite_log==True:
        self.log = open(logfile,'w')
        self.log.write(';'.join(header))
      else:
        self.log = open(logfile,'a')
    else:
      self.log = open(logfile,'w')
      self.log.write(';'.join(header))
    ## load log
    with open(logfile,'r') as f: # open file

      l = f.read().split('\n') # read and split file by newlines.
      ## set id
      if len(l)<=1:
        self.id = 0
      else:
        self.id = int(l[-1][0])+1

  def get(self,url,project_name):
    """Method for connector reliably to the internet, with multiple tries and simple error handling, as well as default logging function.
    Input url and the project name for the log (i.e. is it part of mapping the domain, or is it the part of the final stage in the data collection).

    Keyword arguments:
    url -- str, url
    project_name -- str, Name used for analyzing the log. Use case could be the 'Mapping of domain','Meta_data_collection','main data collection'.
    """

    project_name = project_name.replace(';','-') # make sure the default csv seperator is not in the project_name.
    if self.connector_type=='requests': # Determine connector method.
      for _ in range(self.n_tries): # for loop defining number of retries with the requests method.
        ratelimit()
        t = time.time()
        try: # error handling
          response = self.session.get(url,timeout = self.timeout) # make get call

          err = '' # define python error variable as empty assumming success.
          success = True # define success variable
          redirect_url = response.url # log current url, after potential redirects
          dt = t - time.time() # define delta-time waiting for the server and downloading content.
          size = len(response.text) # define variable for size of html content of the response.
          response_code = response.status_code # log status code.
          ## log...
          call_id = self.id # get current unique identifier for the call
          self.id+=1 # increment call id
          #['id','project_name','connector_type','t', 'delta_t', 'url', 'redirect_url','response_size', 'response_code','success','error']
          row = [call_id,project_name,self.connector_type,t,dt,url,redirect_url,size,response_code,success,err] # define row to be written in the log.
          self.log.write('\n'+';'.join(map(str,row))) # write log.
          return response,call_id # return response and unique identifier.

        except Exception as e: # define error condition
          err = str(e) # python error
          response_code = '' # blank response code
          success = False # call success = False
          size = 0 # content is empty.
          redirect_url = '' # redirect url empty
          dt = t - time.time() # define delta t

          ## log...
          call_id = self.id # define unique identifier
          self.id+=1 # increment call_id

          row = [call_id,project_name,self.connector_type,t,dt,url,redirect_url,size,response_code,success,err] # define row
          self.log.write('\n'+';'.join(map(str,row))) # write row to log.
    else:
      t = time.time()
      ratelimit()
      self.browser.get(url) # use selenium get method
      ## log
      call_id = self.id # define unique identifier for the call.
      self.id+=1 # increment the call_id
      err = '' # blank error message
      success = '' # success blank
      redirect_url = self.browser.current_url # redirect url.
      dt = t - time.time() # get time for get method ... NOTE: not necessarily the complete load time.
      size = len(self.browser.page_source) # get size of content ... NOTE: not necessarily correct, since selenium works in the background, and could still be loading.
      response_code = '' # empty response code.
      row = [call_id,project_name,self.connector_type,t,dt,url,redirect_url,size,response_code,success,err] # define row
      self.log.write('\n'+';'.join(map(str,row))) # write row to log file.
    # Using selenium it will not return a response object, instead you should call the browser object of the connector.
    ## connector.browser.page_source will give you the html.
      return call_id

################################
#        Import packages       #
################################
# Importing standard packages
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import scraping_class
import re

#Initialization of selenium
path2gecko = '/Users/MacbookJos/git/geckodriver' # define path to your geckodriver
browser = webdriver.Firefox(executable_path=path2gecko) # start the browser with a path to the geckodriver.

# direction to boliga.dk
browser.get('https://www.boliga.dk/salg/resultater?salesDateMin=1995&zipcodeFrom=1000&zipcodeTo=2499&searchTab=1&page=1&sort=date-a')
cookie_button = browser.find_element_by_xpath('//*[@id="coiAccept"]')
cookie_button.click()

def df2table(html):
    df = pd.read_html(html)[0]
    df = df.iloc[:, 0:8]
    for column in list(df.columns):
        df[column] = df.loc[column].str.replace(column, '')
    df.columns = ["Address", "Sell_price", "Date_of_sale", "Type",
                  "sqm_price", "Rooms", "m2", "Building_year"]

    #  Removing dublicates of the type of residence
    for i in range(len(df['Type'])):
        for j in ['EEjerlejlighed', 'VVilla', 'RRækkehus']:
            df['Type'][i] = df['Type'][i].replace(j,'')

    # Changing the prices, so that it's numeric values
    for s in range(len(df['Sell_price'])):
        df['Sell_price'][s] = df['Sell_price'][s].replace('.','')
        df['sqm_price'][s] = df['sqm_price'][s].replace('.','')

    df['sqm_price'] = pd.sqm_price.astype('float').dtypes
    df['Sell_price'] = pd.Self_price.astype('float').dtypes
    df['Date_of_sale'] = pd.to_datetime(df.Date_of_sale, format='%d-%m-%Y')

    return df

i = 1
dir = str(os.getcwd())
while i <= 10:
    ## Initialization of log
    logfile = 'log_boliga_csv'
    project_name = 'SDS exam'
    header = ['id','project','connector_type','t', 'delta_t', 'url',\
              'redirect_url','response_size', 'response_code','success',                      'error']

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
    try: #defines error handling
        """
        NOTE: several indicators are different than from the indicators
        that are established with the requests package. `dt`, does not
        necessarily reflect the complete load time. `size` might not  be
        correct as selenium works in the background and could still be
        loading
        """
        html = browser.page_source

        ## Key arguments
        err = ''                           # define python error variable as empty assumming success.
        success = True                     # define success variable
        connector_type = "selenium"
        redirect_url = browser.current_url # log current url, after potential redirects
        dt = t - time.time()               # define delta-time waiting for the server and downloading content.
        size = len(browser.page_source)    # define variable for size of html content of the response.
        response_code = ''                 # log status code.
        ## log...
        call_id = i                        # get current unique identifier for the call

        # Defines row to be written in the log
        row = [call_id, project_name, connector_type, t, dt,\
               redirect_url, size, response_code, success, err] # define row

        log.write('\n'+';'.join(map(str,row))) # write row to log file.
        log.flush()

        # storing parsed table as csv
        df = df2table(html)
        file_path = str(dir + '/boliga' +'/scrape' +'/%d.csv'%i)
        with open(file_path, mode='w', encoding='UTF-8',
                  errors='strict', buffering=1) as f:
            f.write(df.to_csv())
        time.sleep(2)

    except Exception as e:          # define error condition
        log.flush()

    xpath_next = '/html/body/app-root/app-sold-properties-list/div[2]/app-pagination/div[1]/div[4]/a'
    next_button = browser.find_element_by_xpath(xpath_next)
    next_button.click()
    time.sleep(1)
    i += 1

##############################
#    Apppending csv-files    #
##############################

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

####################################################
#      Scraping from wiki for train stations       #
####################################################

####################################################
#                  Metro stations                  #
####################################################
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

##########################################################
#                    S-train stations                    #
##########################################################
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

#################################################################
#                                                               #
#          Reliability and quality of the data                  #
#                                                               #
#################################################################

## The following code seks to investigate the reliability and quality of the data ##
# Importing the relevant packages
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import random

##################################################################
#                                                                #
#       Random sample inspection of our data                     #
#                                                                #
##################################################################

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

# Histogram of the distrubution of square meters
# m2_distribution = boliga_data['m2'].hist(bins=20)
# plt.show()

##########################################################
#                                                        #
#              Visulization of the log file              #
#                                                        #
##########################################################

# Loading the logfile as a pandas dataframe
log_df = pd.read_csv('Scraping_file.csv', sep=';')

# Converts into datetime
log_df['dt'] = pd.to_datetime(log_df.t, unit='s') #unit is seconds

# Visualization of the time it took to make the call for data
# plt.figure(figsize = (20, 6))
# plt.plot(log_df['dt'], log_df.delta_t)
# plt.ylabel('Delta t')
# plt.xlabel('Scraping process')
# plt.title('The time it took to make the call for data')
# plt.show()

# Visualization of the response size through the scraping process
# plt.style.use('ggplot')
# plt.figure(figsize = (20, 6))
# plt.plot(log_df.dt, log_df.response_size)
# plt.ylabel('Response size')
# plt.xlabel('Scraping process')
# plt.title('The response size through the scraping process')
# plt.show()

##########################################################
#                                                        #
#             geolocation parser for houses              #
#                                                        #
##########################################################

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

#####################################################
#                                                   #
#                 Monthly data                      #
#                                                   #
#####################################################

monthly_price = boliga_data.sqm_price.resample('M').mean()
monthly_price['monthly change'] = monyhly_price.pct_change(periods=1, axis=0)
