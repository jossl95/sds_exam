import requests
import pandas as pd
from bs4 import BeautifulSoup
import re

###############################################################################
# M-Stations

# scrape initial table of stations
url = "https://en.wikipedia.org/wiki/List_of_Copenhagen_Metro_stations"
html = requests.get(url).text

# parse table
table = pd.read_html(html)[1]
table = table.drop(['Transfer', 'Line'], axis = 1)
table['Station'] = table['Station'].str.translate({ord('#'): '', ord('†'): ''})

# importing meta-data on stations
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

# parsing location data in degrees with decimals
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

latitude = [parse_dms(lat[0]) for lat in latitude]
longitude = [parse_dms(long[0]) for long in longitude]

df_location = pd.DataFrame([mstation, longitude, latitude]).T
df_location.columns = ['Station', 'Longitude', 'Latitude']
df_mstation = pd.merge(table, df_location, on='Station')

###############################################################################
# S-Stations

# scrape initial table of stations
url = "https://en.wikipedia.org/wiki/List_of_Copenhagen_S-train_stations"
html = requests.get(url).text

# parse table
table = pd.read_html(html)[1]
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

latitude = [parse_dms(lat[0]) for lat in latitude]
longitude = [parse_dms(long[0]) for long in longitude]

df_location = pd.DataFrame([sstation, longitude, latitude]).T
df_location.columns = ['Station', 'Longitude', 'Latitude']
df_sstation = pd.merge(table, df_location, on='Station')

df_sstation
###############################################################################
# print DataFrames
print(df_mstation)
print(df_sstation)
