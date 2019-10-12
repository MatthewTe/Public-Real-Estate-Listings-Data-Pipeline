# Importing scraping packages:
import bs4
import requests
# Importing data managment packages:
import pandas as pd

'''
Script contains a set of objects meant to represent the RE listings data scraped
from various public websites.

The main function of these objects will be as a means of integerating the scraped
data into the listings database via database connection objects and methods.
'''

class Kijiji(object):
    """
    This object is mean to represent the data scraped from the canadian listings
    website Kijiji.ca.

    The object will contain various methods for selecting
    specific instances of data scraped from the site however the objects focus
    will be on itterating through the listings website's various pages and
    extracting all listings data into a large dataframe given a Kijiji url as a
    starting point.

    Parameters
    ----------
    init_url : str
        This url will serve as the starting point for the web scraping methods in
        this object. Using this url as a starting point it will itterate through
        the number of pages specificed by the page variable, constructing a
        large dataframe of individual listings

    page : int
        An integer that specifies the number of pages the web scraping methods
        will itterate through in constructing the main dataframe.

    """

    def __init__(self, init_url, page):

        # Declaring instance variables:
        self.inital_url = init_url
        self.page = page

        # Instance of main listings dataframe:
        self.data = self.build_main_dataframe()

    def build_main_dataframe(self):
        '''This main method of the Kijiji object uses several other Kijiji object
        methods to construct the main dataframe of listings scraped from
        the inital Kijiji.ca url.

        Returns
        -------
        main_listings_dataframe : pandas dataframe
            The main dataframe that contains all the listings data scraped
            from Kijiji.ca
        '''

    def page_to_dataframe(self, url):
        '''This method parses a Kijiji.ca RE listings page and converts all
        listings shown on the page to a pandas dataframe

        Parameters
        ----------
        url : str
            This is the Kijiji.ca listings url that will be parsed and converted
            to a dataframe.

        Returns
        -------
        page_dataframe : pandas dataframe
            The dataframe containing all the RE listings found on the Kijiji.ca
            page

        '''
        pass

    def get_next_url(self, url):
        '''This method parses a Kijiji.ca RE listings page and extracts and
        returns the url to the next Kijiji listings page

        Parameters
        ----------
        url : str
            This is the Kijiji.ca url that will be paresed and the 'next url'
            extracted

        Returns
        -------
        next_url : str
            This is the url to the next Kijiji.ca RE listings page
        '''

        pass
