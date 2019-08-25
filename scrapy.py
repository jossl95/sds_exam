from selenium import webdriver
import time
import os
import pandas as pd
import Connector
import glob

#######################
#    Scraping data    #
#######################

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
