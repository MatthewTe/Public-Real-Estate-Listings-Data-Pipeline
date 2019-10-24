# Importing all ETF subpackages:
from data_extraction_pkg.mySQL_database_connector import Real_Estate_Listingsdb as mySQLdb
from data_extraction_pkg.web_data_models import Kijiji
from data_transformation_pkg.geoprocessor import data_transform


class pipeline(object):
    """
    This is the main class of the ETL pipeline package. It combines the
    extraction and transformation sub-packages into a series of processes that
    allow for the creation of SQL tables, the extraction of raw data from the
    web data models, the transformation of data and storage.

    The methods of this object centre around creating an maintaining specific
    data tables within the pipeline.

    Parameters
    ----------
    city : str
        This is the city that the pipeline will be created for. The acutal area
        does not need to be specified beyond the city name as it is accompanied
        by a Kijiji url.

    url : str
        This is the correspoinding Kijiji url that links to the listings page
        for the location of the city.


    db_parms : dict
        This is a dictionary containing all the connection information for the
        online database service that is being used to read and write data.
    """

    def __init__(self, city, url, db_parms):
        # Declaring instance variables:
        self.city = city
        self.url = url
        self.db_parms = db_parms

        # Constructing table: names:
        self.rawtbl_name = city + '_raw_tbl'
        self.maintbl_name = city + '_maintbl'

        # Initalizing connection with the database for raw data:
        Intialdb_connection = mySQLdb(self.db_parms['host'], self.db_parms['user'], self.db_parms['passwd'],
        self.db_parms['db_name'], self.rawtbl_name)


        # Creating raw data table if none exists:
        createtbl_query = "CREATE TABLE IF NOT EXISTS {} (\
Address TEXT,\
Price TEXT,\
Date TEXT,\
Bedrooms TEXT,\
Bathrooms TEXT,\
Size TEXT)".format(self.rawtbl_name)


        # Creating curor object:
        self.cur = Intialdb_connection.db.cursor()

        # performing SQL query:
        self.cur.execute(createtbl_query)

        # Creating raw data table if none exists:
        createtbl_query = "CREATE TABLE IF NOT EXISTS {} (\
Address TEXT,\
Price FLOAT,\
Date DATE,\
Bedrooms FLOAT,\
Bathrooms FLOAT,\
Size FLOAT,\
Coordinates TEXT)".format(self.maintbl_name)


        # Executing command:
        self.cur.execute(createtbl_query)

    # TODO: Physically Map out what this method does currently

    # TODO: Create a method that maintains the database, updating it a certain number of times.

    



# Test:
db_parms = {'host': 'localhost', 'user': 'Main_User', 'passwd': '', 'db_name': 're_listings_data'}
pipeline('Toronto', 'https://www.kijiji.ca/b-house-for-sale/gta-greater-toronto-area/c35l1700272?ll=', db_parms)
