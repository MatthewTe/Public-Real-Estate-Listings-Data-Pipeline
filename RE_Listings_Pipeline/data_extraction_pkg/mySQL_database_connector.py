# Package sys.path hack:
import sys
sys.path.append("..")
# Importing MySQL database connector packages:
import MySQLdb
# Importing data managment packages:
import pandas as pd
# Importing web data models: # NOTE: hack requiring absoloute import:
from RE_Listings_Pipeline.data_extraction_pkg.web_data_models import Kijiji

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
        for index, row in unique_df.iterrows():

            # building SQL execute string:
            add_row = """INSERT INTO %s (Address, Price, Date, Bedrooms, Bathrooms, Size)\
 VALUES ( "{}", "{}", "{}", "{}", "{}", "{}" )""" % (self.table_name)


            add_row = add_row.format(row['Address'], row['Price'], row['Date'],
            row['Bedrooms'], row['Bathrooms'], row['Size'])


            # Executing SQL command:
            self.cur.execute(add_row)


        # Comming entries to database:
        self.db.commit()

        # Closing connection:
        self.db.close()

    def read_data(self, num_rows):
        '''This method queries the MySQL server and extracts data from the
        data table specified by the Real_Estate_Listingsdb() __init__ method
        based on the number of rows specified

        Parameters
        ----------
        num_rows : int
            The integer specifying the number of rows that will be queried from
            the SQL table

        Returns
        -------
        df : pandas dataframe
            This is the dataframe containing the data queried from the SQL
            database
        '''

        # Constructing SQL query string:
        query_string = 'SELECT * FROM %s LIMIT %d' % (self.table_name, num_rows)

        # Converting the SQL query into a dataframe:
        df = pd.read_sql(query_string, con= self.db)


        return df

    def update_transformtbl(self, data):
        '''This method is used to update the SQL table containing the raw data
        after it has been transformed via the geoprocessing package.

        Parameters
        ----------
        data : pandas dataframe
            This dataframe contains the end reuslt of the data transformation
            process

        '''

        # Declaring the input data as update_df:
        update_df = data

        # Extracting and building a dataframe from all current data in the
        # 'main_listings_tbl' table:
        main_db_dataframe = pd.read_sql('SELECT * FROM %s' % self.table_name
        , con=self.db)

        # Merging the main SQL database with the current Kijiji scraped database:
        interim_df = pd.concat([main_db_dataframe, update_df], ignore_index=True)

        # Selecting only the unique valeus within the dataframe:
        unique_df = interim_df.drop_duplicates()


        # Adding all rows of unique_df to the SQL database:
        for index, row in unique_df.iterrows():

            # building SQL execute string:
            add_row = """INSERT INTO %s (Address, Price, Date, Bedrooms,\
Bathrooms, Size, Coordinates)
VALUES ( "{}", {}, "{}", {}, {}, {}, "{}" )""" % (self.table_name)


            add_row = add_row.format(row['Address'], row['Price'], row['Date'],
            row['Bedrooms'], row['Bathrooms'], row['Size'], row['Coordinates'])


            # Executing SQL command:
            self.cur.execute(add_row)


        # Comming entries to database:
        self.db.commit()

        # Closing connection:
        self.db.close()
