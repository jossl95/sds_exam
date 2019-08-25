import time
import os
import pandas as pd
import Connector

#Initialization of selenium
path2gecko = '/Users/MacbookJos/git/geckodriver' # define path to your geckodriver
browser = Connector('log_boliga.csv',
                    connector_type= 'selenium',
                    path2selenium='/Users/MacbookJos/git/geckodriver')

# direction to boliga.dk
browser.get('https://www.boliga.dk/salg/resultater?salesDateMin=1995&zipcodeFrom=1000&zipcodeTo=2499&searchTab=1&page=1&sort=date-a', "SDS student")
cookie_button = browser.find_element_by_xpath('//*[@id="coiAccept"]')
cookie_button.click()

def html2table(html):
    df = pd.read_html(html)[0]
    df = df.iloc[:, 0:8]
    for column in list(df.columns):
        df[column] = df[column].str.replace(column, '')
    df.columns = ['Address', "Sell_price", "Date_of_sale", "Type",
                  "sqm_price", "Rooms", "m2","Building_year"]

    #  Removing dublicates of the type of residence
    for i in range(len(df['Type'])):
        for j in ['EEjerlejlighed', 'VVilla', 'RRækkehus']:
            df['Type'][i] = df['Type'][i].replace(j,'')

    # Changing the prices, so that it's numeric values
    for s in range(len(df['Sell_price'])):
        df['Sell_price'][s] = df['Sell_price'][s].replace('.','')
        df['sqm_price'][s] = df['sqm_price'][s].replace('.','')

    df['sqm_price'] = pd.to_numeric(df['sqm_price'])
    df['Sell_price'] = pd.to_numeric(df['Sell_price'])

    # Changing the date into date-time format
    def format_dates(date):
        q = date.split('-')
        return q[1].strip() + q[0].strip() + q[2]

    df['Date_of_sale'] = df['Date_of_sale'].apply(lambda x: format_dates(x))
    df['Date_of_sale'] = pd.to_datetime(df['Date_of_sale'], format='%m%d%Y')

    return df

i = 1
dir = str(os.getcwd())
while i <= 1:
    df = html2table(html = browser.page_source)
    file_path = str(dir + '/boliga' +'/scrape' +'/%d.csv'%i)
    with open(file_path, mode='w', encoding='UTF-8',
              errors='strict', buffering=1) as f:
        f.write(df.to_csv())
    time.sleep(2)

    xpath_next = ' /html/body/app-root/app-sold-properties-list/div[2]/app-pagination/div[1]/div[4]/a'
    next_button = browser.find_element_by_xpath(xpath_next)
    next_button.click()
    time.sleep(1)
    i += 1
