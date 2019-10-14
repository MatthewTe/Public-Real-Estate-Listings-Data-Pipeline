# Importing MySQL database connector packages:
import MySQLdb
# Importing data managment packages:
import pandas as pd
# Importing web data models:
from web_data_models import Kijiji

class Real_Estate_Listingsdb(object):
    '''
    This object is meant to provide a means of connecting a MySQL database to the
    web data models that collect data from public listings.

    The input parameters are used to build a MySQLdb.connect() object.

    Parameters
    ----------
    host : str
        The host location of the db

    user : str
        The name of the user that will be used to connect to the database

    password : str
        The password to the database

    db : str
        The name of the database that will be connected to

    table_name : str
        This is the name of the table within the database that will be accessed
        /created/mainpulated for each session
    '''

    def __init__(self, host, user, password, db_name, table_name):
        # Declaring instance variables:
        self.host = host
        self. user = user
        self. password = password
        self.db_name = db_name
        self.table_name = table_name

        # Building instances of the connector and cursor objects:
        self.db = MySQLdb.connect(host = self.host,
                                    user = self.user,
                                    passwd = self.password,
                                    db = self.db_name)
        # Instance cursor variable:
        self.cur = self.db.cursor()


        # Creating the necessary listings table within the database if it does
        # not exist: # NOTE: Assumes database already exists.

        # Declaring the CREATE TALBE IF NOT EXIST SQL command string:
        createtbl_cmd = ("CREATE TABLE IF NOT EXISTS %s (\
Address TEXT (250) ,\
Price TEXT,\
Date TEXT,\
Bedrooms TEXT,\
Bathrooms TEXT,\
Size TEXT )" % self.table_name)

        # Executing the create table command:
        self.cur.execute(createtbl_cmd)

    def update_Kijiji(self, Kijiji_object):
        '''This is the main method that will be used to maintain the RE listings
        database. It accepts a Kijiji().data object as input and updates the
        MySQL database with the unqiue listing enteries scraped from Kijiji.ca

        It does this by performing a .fetchall() request for all the values
        currently in the MySQL db and converts them to a pandas dataframe. It
        then compares this dataframe with the newly built dataframe, removes
        duplicate values between the two, createing dataframe of only new data.
        This new data is then pushed to the main SQL table and appended to it.

        Parameters
        ----------
        Kijiji_object : pandas dataframe
            This is the pandas dataframe of newly scraped listings data built
            by the Kijiji() data model
        '''

        # Decalring the Kijiji_object dataframe:
        update_df = Kijiji_object

        # Extracting and building a dataframe from all current data in the
        # 'main_listings_tbl' table:
        main_db_dataframe = pd.read_sql('SELECT * FROM %s' % self.table_name
        , con=self.db)

        # Merging the main SQL database with the current Kijiji scraped database:
        interim_df = pd.concat([main_db_dataframe, update_df], ignore_index=True)

        # Selecting only the unique valeus within the dataframe:
        unique_df = interim_df.drop_duplicates()


        # Adding all rows of unique_df to the SQL database:
        
        # TODO: Write code that Inserts unique_df into the MySQL table.



# For Testing:
single_page = Kijiji('https://www.kijiji.ca/b-house-for-sale/gta-greater-toronto-area/c35l1700272', 1).data
Test = Real_Estate_Listingsdb('localhost', 'Python_ETL_connector',
 'q$.)dzFQGeZaK6c', 're_listings_data', 'main_listings_tbl').update_Kijiji(single_page)
