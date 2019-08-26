# import packages
from geopy.distance import geodesic
from dateutil.parser import parse
from geopy.geocoders import Nominatim

#parse the opening year from int to string to datetime
df_sstation['Opened'] = df_sstation['Opened'].apply(lambda x: str(x))
df_mstation['Opened'] = df_mstation['Opened'].apply(lambda x: str(x))
df_sstation['Opened'] = df_sstation['Opened'].apply(lambda x: parse(x))
df_mstation['Opened'] = df_mstation['Opened'].apply(lambda x: parse(x))
#parse the date of sale from to datetime
final_data['Date_of_sale'] = final_data['Date_of_sale'].apply(lambda x: str(x))
final_data['Date_of_sale'] = final_data['Date_of_sale'].apply(lambda x: parse(x))

#calculate which station were open at the year of the sale
def was_opened(property_, stations):
    open_stations = dict()
    for i in range(len(stations)):
        if (stations.iat[i,2] < property_[2]) == True:
            open_stations[stations.iat[i,0]] = (abs(stations.iat[i,-1]),abs(stations.iat[i,-2]))
    return open_stations

#calculate distance to closeste open station
def get_distance_opened(property_, df_stations):
    stations = was_opened(property_, df_stations)
    if  stations == {}:
        return None, None
    property_loc = (property_[-2], property_[-1])
    if property_loc[0] is not None or property_loc[1] is not None:
        min_dist = 999999999999999999999
        min_dist_station = ''
        for station, station_loc in stations.items():
            dist = geodesic(station_loc, property_loc).km
            if dist < min_dist:
                min_dist = dist
                min_dist_station = station
        return round(min_dist,3), min_dist_station
    else:
        return None, None

def was_not_opened(property_, stations):
    open_stations = dict()
    for i in range(len(stations)):
        if (stations.iat[i,2] > property_[2]) == True:
            open_stations[stations.iat[i,0]] = (abs(stations.iat[i,-1]),abs(stations.iat[i,-2]))
    return open_stations


def get_distance_not_opened(property_, df_stations):
    stations = was_not_opened(property_, df_stations)
    if  stations == {}:
        return None, None
    property_loc = (property_[-2], property_[-1])
    if property_loc[0] is not None or property_loc[1] is not None:
        min_dist = 999999999999999999999
        min_dist_station = ''
        for station, station_loc in stations.items():
            dist = geodesic(station_loc, property_loc).km
            if dist < min_dist:
                min_dist = dist
                min_dist_station = station
        return round(min_dist,3), min_dist_station
    else:
        return None, None


geolocator = Nominatim(user_agent="Social Data Science Student")
copenhagen = geolocator.geocode("Copenhagen")[-1]

def dist_city_center(property_):
    property_loc = (property_[-2], property_[-1])
    if property_loc[0] is not None or property_loc[1] is not None:
        return round(geodesic( property_loc, copenhagen).km, 3)
    else:
        return None
