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
        online database service that is being used to read and write data. The
        structure of the db_parms dictionary is:

        {'host', 'user', 'passwd', 'db_name'}

    """

    def __init__(self, city, url, db_parms):
        # Declaring instance variables:
        self.city = city
        self.url = url
        self.db_parms = db_parms

        # Constructing table: names:
        self.rawtbl_name = city + '_rawtbl'
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
Size TEXT,\
PRIMARY KEY (Address(255)))".format(self.rawtbl_name)


        # Creating curor object:
        self.cur = Intialdb_connection.db.cursor()

        # performing SQL query:
        self.cur.execute(createtbl_query)

        # Creating raw data table if none exists:
        createtbl_query = "CREATE TABLE IF NOT EXISTS {} (\
Address VARCHAR(255),\
Price FLOAT,\
Date DATE,\
Bedrooms FLOAT,\
Bathrooms FLOAT,\
Size FLOAT,\
Coordinates TEXT,\
PRIMARY KEY (Address(255)))".format(self.maintbl_name, self.rawtbl_name)


        # Executing command:
        self.cur.execute(createtbl_query)

    def tbl_update(self, num_pages, api_key):
        '''This method utilizes the Kijiji data models and the data_extraction_pkg
        to update and maintain the raw data table for the specified Kijiji listings
        pipeline.

        It then uses the same data extracted form the Kijiji data model and
        processes it using the geoprocessing package. It then writes the processed
        data into the SQL database into the maintbl.

        Parameters
        ----------
        num_pages : int
            The number of listings pages that will be used to update the
            database based on the inital Kijiji url.

        api_key : str
            This is the API key that will be used to acess the online geoprocessor
            for the raw data.
        '''

        # Creating dataframe containing all Kijiji listings from Kijiji data model:
        Kijiji_data_model = Kijiji(self.url, num_pages)

        # Placing the dataframe into the data_extraction_pkg:
        raw_listings_db = mySQLdb(self.db_parms['host'], self.db_parms['user'],
         self.db_parms['passwd'], db_parms['db_name'], self.rawtbl_name)

        # Updating rawtbl listings data with data scraped by Kijiji data model:
        raw_listings_db.update_Kijiji(Kijiji_data_model.data)

        # Creating database connection for the main data tbl:
        main_listings_tbl = mySQLdb(self.db_parms['host'], self.db_parms['user'],
         self.db_parms['passwd'], db_parms['db_name'], self.maintbl_name)


        # Inputting the scraped data model into the geoprocessor:
        main_table = data_transform(Kijiji_data_model.data, api_key)


        # Writing the geoprocessed data into the main data table:
        main_listings_tbl.update_transformtbl(main_table.data)
