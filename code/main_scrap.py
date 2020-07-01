import pandas as pd
import numpy as np
import os 
import sys
import collections
from collections import defaultdict
import requests
import tqdm
from bs4 import BeautifulSoup
import re
import psycopg2

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options

#Selenium options
options = Options()
options.add_argument("--headless")
options.add_argument("window-size=1400,1500")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("start-maximized")
options.add_argument("enable-automation")
options.add_argument("--disable-infobars")
options.add_argument("--disable-dev-shm-usage")


# Input
first_input = int(sys.argv[1]) # number of the df row to start from the queries.csv
second_input = int(sys.argv[2]) # number of the df row to finish from the queries.csv

# List of indexes from first to second (both included)
list_input = list(range(first_input, second_input + 1))

# Read the queries dataframe
df = pd.read_csv("/home/ec2-user/scrap/data/queries.csv", delimiter = ";")
df.set_index("index", inplace=True)

### -------------------------------------- ###
#           SCRAP FUNCTIONS
### -------------------------------------- ###
# Clean string
def clean_string(st):
    st = st.lower()
    st = st.replace(","," ")
    st = st.replace(";", " ")
    st = st.replace("&", " ")
    st = st.replace("[", " ")
    st = st.replace("]", " ")
    st = st.replace("-", " ")
    st = st.replace("'", " ")
    st = st.replace("à","a")
    st = st.replace("á", "a")
    st = st.replace("ä", "a")
    st = st.replace("é", "e")
    st = st.replace("ë", "e")
    st = st.replace("è", "e")
    st = st.replace("í", "i")
    st = st.replace("ï", "i")
    st = st.replace("ò", "o")
    st = st.replace("ó", "o")
    st = st.replace("ö", "o")
    st = st.replace("ú", "u")
    st = st.replace("ü", "u")
    return st

def split_string(st):
    return [particle for particle in st.split(" ") if len(particle)>0]

def match_title(queried, qtrack, qartist, a, b):
    """
    a = title.find("a").get("title") (text that appears on the title)
    b = artist_yt.text (text that appears on the artist section)
    queried = track_name + " " + artist_name
    qtrack = track_name
    qartist = artist_name
    """
    
    # Clearning
    a = clean_string(a)
    b = clean_string(b)
    queried = clean_string(queried)
    qtrack = clean_string(qtrack)
    qartist = clean_string(qartist)
    
    # Splitting
    a = set(split_string(a))
    b = set(split_string(b))
    queried = set(split_string(queried))
    qtrack = set(split_string(qtrack))
    qartist = set(split_string(qartist))
    
    # Matching
    match = False
    lquery = len(queried) #length of total query
    ltrack = len(qtrack) #length of elements of qtrack
    lartist = len(qartist) # length of elements of qartis
    
    # Check match with title
    if len(a & queried) == lquery:
        match = True
    elif len(b & queried) == lartist: # if the artist name coincides
        # if the title contains the track name or at least almost all words except 1
        if len(a & qtrack) >= (ltrack -1): 
            match = True
    elif len(a & queried) == (lquery - 1): # allow one word not to be present
        match = True
        
    # For track_names with more than 3 words, allow some words missing
    if match is False:
        # If more than 3 words in the name of the song
        if ltrack > 3:
            # Allow two words to be missing
            if len(a & qtrack) >= (lquery -2):
                match = True           
        
    return match


### -------------------------------------- ###
#           DB FUNCTIONS
### -------------------------------------- ###
def db_execute_insert(query):
    ENDPOINT="tracksurl.czjs6btlvfgd.eu-west-2.rds.amazonaws.com"
    PORT="5432"
    USR="david"
    REGION="eu-west-2"
    DBNAME="postgres"
    PSSWD=["qrks","jfut","iv","uf","1"]
    
    try:
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=''.join(PSSWD))
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        return True
    except Exception as e:
        error = "Database connection failed due to {}".format(e)
        return False
    
def db_execute_select():
    ENDPOINT="tracksurl.czjs6btlvfgd.eu-west-2.rds.amazonaws.com"
    PORT="5432"
    USR="david"
    REGION="eu-west-2"
    DBNAME="postgres"
    PSSWD=["qrks","jfut","iv","uf","1"]
    
    try:
        conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USR, password=''.join(PSSWD))
        cur = conn.cursor()
        cur.execute("""SELECT * FROM results""")
        query_results = cur.fetchall()
        df = query_results_to_df(query_results)
        return df
    except Exception as e:
        error = "Database connection failed due to {}".format(e)
        return False

def query_results_to_df(query_results):
    if len(query_results) == 0:
        return False
    cols = ["artist_id","track_id","is_found","queried","href", "visual"]
    return pd.DataFrame(query_results, columns=cols)


### --------------------------------------------------------------- ###
### --------------------------------------------------------------- ###
### --------------------------------------------------------------- ###


#           ITERATION over songs


### --------------------------------------------------------------- ###
### --------------------------------------------------------------- ###
### --------------------------------------------------------------- ###

# Before iterating, start the webbrowser
browser = webdriver.Chrome(options=options)

for index_input in list_input:
    # Take the row of the dataframe
    df_iter = df.loc[index_input]

    # Get the properties of that row
    artist_name = df_iter.artist_name
    artist_id = df_iter.artist_id
    track_name = df_iter.track_name
    track_id = df_iter.track_id

    ### -------------------------------------- ###
    #     Input is TRACK_NAME, ARTIST_NAME
    ### -------------------------------------- ###
    input_query = [track_name, artist_name]
    qtrack  = track_name
    qartist = artist_name
    queried = qtrack + " " + qartist

    # -------------------------------------------------------------------
    # OPTIONAL: Check if that pair artist-track already has been done
    # df_current_urls = db_execute_select()
    # mask_artist = df_current_urls["artist_id"] == artist_id
    # mask_track = df_current_urls["track_id"] == track_id
    # shape_result = df_current_urls[mask_artist & mask_track].shape[0]
    # if shape_result > 0:
    #     #sys.exit("Already done") # TO DO
    #     print("Already Done")
    # -------------------------------------------------------------------


    ### -------------------------------------- ###
    #           SCRAPPING
    ### -------------------------------------- ###

    # Create the query
    qq = f"https://www.youtube.com/results?hl=es&gl=ES&search_query={queried}"

    # Perform query
    browser.get(qq)

    # Convert to LXML
    page = BeautifulSoup(browser.page_source, 'lxml')

    # --------------------------------------------------------------------------------------------------- #
    # WRAPPER LIST OF VIDEOS

    # Take the text wrapper class that has both the title, the artist name and the metadata (visualizations)
    # This returns a list of all the text wrappers
    text_wrapper_list = page.find_all("div", {"class": "text-wrapper style-scope ytd-video-renderer"})
    # --------------------------------------------------------------------------------------------------- #

    # --------------------------------------------------------------------------------------------------- #
    # FOR EACH VIDEO WRAPPER

    # for loop to navigate to each text wrapper
    for title_wrapper in text_wrapper_list:


        ### ---------  VIDEO TITLE and HREF  ----------- ###
        video_title_a = title_wrapper.find("a", {"id": "video-title"})
        yt_title = video_title_a.attrs["title"]
        yt_href = video_title_a.attrs["href"]

        ### ---------  VIDEO ARTIST CHANNEL   ----------- ###
        channel_a = title_wrapper.find("a", {"class": "yt-simple-endpoint style-scope yt-formatted-string"})
        yt_artist = channel_a.text

        ### ---------  TOTAL VISUALIZATIONS   ----------- ###
        aria_label_visualizations = str(video_title_a).replace(".","")
        visualizations = set(re.findall(r"(\d+) visualizaciones", aria_label_visualizations))
        if len(visualizations):
            visualizations = list(visualizations)[0]
            visualizations = int(visualizations)
        else:
            visualizations = -1
            
        # If visualizations = -1 maybe it's because instead of visualizaciones it is set set "views"
        # hence views, the milliards are separated by commas
        aria_label_visualizations = str(video_title_a).replace(",","")
        find_views = set(re.findall(r"(\d+) views", aria_label_visualizations))
        if len(find_views):
            find_views = list(find_views)[0]
            visualizations = int(find_views)
        ### ---------  CHECK IF MATCH   ----------- ###

        # Cleaning strings
        match = match_title(queried, qtrack, qartist, yt_title, yt_artist)
        
        if match:
            break



    ### -------------------------------------- ###
    #           UPLOAD RESULTS
    ### -------------------------------------- ###

    # Prepare the variables
    is_found = 'f'
    href_video = "NOTFOUND"
    queried = queried.lower()

    # If a match has been found, convert URL to the full URL adding the href
    if match:
        if len(yt_href):
            is_found = 't'
            href_video = "https://www.youtube.com" + yt_href

    # Query to insert results with the href
    query_insert = f"""
    INSERT INTO results VALUES ('{artist_id}','{track_id}','{is_found}','{queried}','{href_video}',{visualizations})
    """.strip()

    # Execute query
    result_query = db_execute_insert(query_insert)