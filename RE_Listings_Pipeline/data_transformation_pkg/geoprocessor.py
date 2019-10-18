# Path hack:
import sys
sys.path.append("..")

# Importing data managment/manipulation packages:
import pandas as pd
import re

# Importing database connector from data_extraction_pkg:
from RE_Listings_Pipeline.data_extraction_pkg.mySQL_database_connector import \
Real_Estate_Listingsdb as MySQLdb
from RE_Listings_Pipeline.data_extraction_pkg.web_data_models import Kijiji
# Importing Geocoding packages:
from geopy import OpenCage
from geopy.extra.rate_limiter import RateLimiter



class data_transform(object):
    """
    This object is meant to contain all the necessary data transformation methods
    and builds the completed transformed dataframe.

    Parameters
    ----------
    raw_data : pandas dataframe
        This is the raw RE listings data that is pulled from the SQL database
        via the mySQL_database_connector package

    api_key : str
        This is the api key that will be used to access the geocoder api to perform
        the geoprocessing data. For the moment this method uses the geocoder api
        OpenCage
    """

    def __init__(self, raw_data, api_key):
        # Declaring the instance data:
        self.raw_data = raw_data
        self.api_key = api_key

        # Declaring instance of geocoder: OpenCage
        self.geolocator = OpenCage(self.api_key)
        # Adding rate limiter to avoid API bottleneck:
        self.geocode = RateLimiter(self.geolocator.geocode, min_delay_seconds=1)

        # Building the transformed dataframe:
        self.data = self.build_geoprocessed_data()


    def build_geoprocessed_data(self):

        # re-deffining raw dataframe:
        df = self.raw_data

        # Performing geotransformation on dataset:

        # Creating a colum of geocode objects to be called to manipulate other colums:
        df['geocode'] = df['Address'].apply(lambda x: self.geocode(x))

        # Adding Coordinate column: tuple of location latitude and longitude
        df['Coordinates'] = df['geocode'].apply(lambda x: (x.latitude, x.longitude))

        # Refactoring the Address column using geocoder formatting:
        df['Address'] = df['geocode'].apply(lambda x: x.address)

        # Dropping geocode table:
        df.drop(['geocode'], axis=1, inplace=True)


        # Performing data formatting:

        # Removing all non numeric characters from the 'Bedrooms', 'Bathrooms
        # columns:
        df['Bedrooms'] = df['Bedrooms'].apply(lambda x: re.sub(r'[^0-9 . NaN]', '', x))

        df['Bathrooms'] = df['Bathrooms'].apply(lambda x: re.sub(r'[^0-9 . NaN]', '', x))

        # replacing every instance of 'NaN' with "0" and 'Please Contact' with 'NaN':
        df.replace(to_replace='NaN', value='0')

        # Removing rows where there is no price listed:
        df = df[df.Price != 'Please Contact']

        # Converting data within df to the correct data type:
        df.Price = df.Price.astype(float).fillna(0.0)
        df.Bedrooms = df.Bedrooms.astype(float).fillna(0.0)
        df.Bathrooms = df.Bathrooms.astype(float).fillna(0.0)
        df.Size = df.Size.astype(float).fillna(0.0)


        return df
