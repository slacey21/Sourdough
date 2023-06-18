# **Purpose:** Script used to scrape data for intern project POC from:
# - **r/Sourdough**  
# - **r/Breadit**  
# - **r/ArtisanBread**  
# - **r/Baking** 

# ## List of Useful Resources
# **for webscraping:** https://www.geeksforgeeks.org/scraping-reddit-using-python/  
# **for troubleshooting & general tips/help:** https://www.stackoverflow.com   
# **PRAW Documentation:** https://praw.readthedocs.io/en/stable/  
# **Pushshift.io (used to get all subreddit info since deprecation of PRAW "subreddit" class):** https://pushshift.io/  
# **for connecting to pushshift.io API**: https://www.jcchouinard.com/how-to-use-reddit-api-with-python/

# 1. Creating an authorized PRAW instance (Python Reddit API Wrapper)
# Using an authorized PRAW instance will allow for greater breadth and depth of data collection


import praw
import pandas as pd
from getpass import getpass

# prompt user for protected password input
pwd = getpass("Please enter password:")

reddit = praw.Reddit(client_id="REDACTED",
                                client_secret="REDACTED",
                                user_agent="REDACTED",
                                username="REDACTED",
                                password=pwd)


# # 2. Get the time created for each subreddit you wish to scrape
# ### This enables scraping of all posts in the subreddit from its creation until any given end point


subs = ["Sourdough", "Breadit", "ArtisanBread", "Baking"]

# Initialize an empty dictionary, the keys will be the subreddit names, and the values will be time created
time_created = {}

for sub in subs:
    time_created[sub] = int(reddit.subreddit(sub).created_utc)



# # 3. Connect to the Pushshift.io API to scrape all subreddit posts

#    ## 3.a. Create user-defined function to scrape a subreddit over a given time period

# In[25]:


import requests
import time

# recursive user-defined function to scrape all posts from a subreddit over a given time period
# one request is limited to 1000 results, so will capture the timestamp of 
# last post and continue until all posts have been collected
def scrape_sub(before, after, subreddit, data):
    
    url = f"https://api.pushshift.io/reddit/search/submission/?subreddit={subreddit}&sort=desc&sort_type=created_utc&after={after}&before={before}&size=1000"
    request = requests.get(url)
    
    while request.status_code != 200:
        request = requests.get(url)
        
    json_response = request.json()
    submissions = json_response["data"]
    data.append(submissions)

    if len(submissions) > 1:
        print(subreddit, min([x["created_utc"] for x in submissions]), before, after, len(submissions), len(data))
        scrape_sub(min([x["created_utc"] for x in submissions])+1, after, subreddit, data)
    
    return data
        


# ### 3.b. Run scraping function over desired subreddits and save data to CSV file for future loading

# #### 3.b.i. r/Sourdough Data

# r/Sourdough data
sd_data_1 = scrape_sub(int(time.time()), 1593573801, "Sourdough", [])
sd_data_2 = scrape_sub(1593573801, time_created["Sourdough"], "Sourdough", [])
sd_data = sd_data_1 + sd_data_2

# flatten out list of list of dictionaries into a list of dictionaries
flat_sd_data = [subitem for item in sd_data for subitem in item]

# convert list of dictionaries to pandas dataframe
sd_df = pd.DataFrame(flat_sd_data)

# write dataframe to CSV file for loading into database in future
sd_df.to_csv(r"REDACTED/Sourdough_Subreddit_Data_20220204.csv", index = False)


# #### 3.b.i. r/Breadit Data
# r/Breadit data
bred_data_1 = scrape_sub(int(time.time()), 1593246800, "Breadit", [])
bred_data_2 = scrape_sub(1593246800, 1553246800, "Breadit", [])
bred_data_3 = scrape_sub(1553246800, time_created["Breadit"], "Breadit", [])

bred_data = bred_data_1 + bred_data_2 + bred_data_3

# flatten out list of list of dictionaries into a list of dictionaries
flat_bred_data = [subitem for item in bred_data for subitem in item]

# convert list of dictionaries to pandas dataframe
bred_df = pd.DataFrame(flat_bred_data)

# write dataframe to CSV file for loading into database in future
bred_df.to_csv(r"REDACTED/Breadit_Subreddit_Data_20220204.csv", index = False)


# #### 3.b.i. r/ArtisanBread Data
# r/ArtisanBread data
ab_data = scrape_sub(int(time.time()), time_created["ArtisanBread"], "ArtisanBread", [])

# flatten out list of list of dictionaries into a list of dictionaries
flat_ab_data = [subitem for item in ab_data for subitem in item]

# convert list of dictionaries to pandas dataframe
ab_df = pd.DataFrame(flat_ab_data)

# write dataframe to CSV file for loading into database in future
ab_df.to_csv(r"REDATCED/ArtisanBread_Subreddit_Data_20220204.csv", index = False)


# #### 3.b.i. r/Baking Data
# r/Baking data
bak_data_1 = scrape_sub(int(time.time()), 1616246800, "Baking", [])
bak_data_2 = scrape_sub(1616246800, 1596246800, "Baking", [])
bak_data_3 = scrape_sub(1596246800, 1556246800, "Baking", [])
bak_data_4 = scrape_sub(1556246800, 1506246800, "Baking", [])
bak_data_5 = scrape_sub(1506246800, time_created["Baking"], "Baking", [])

bak_data = bak_data_1 + bak_data_2 + bak_data_3 + bak_data_4 + bak_data_5

# flatten out list of list of dictionaries into a list of dictionaries
flat_bak_data = [subitem for item in bak_data for subitem in item]

# convert list of dictionaries to pandas dataframe
bak_df = pd.DataFrame(flat_bak_data)

# write dataframe to CSV file for loading into database in future
bak_df.to_csv(r"REDACTED/Baking_Subreddit_Data_20220207.csv", index = False)


# # 4. Filter & Combine Data and Load to Google Cloud

# FILTER & COMBINE DATA INTO ONE FILE
import os
import numpy as np

# relevant fields for analysis
fields = ["author","created_utc","domain","full_link","id","is_crosspostable","is_original_content",
          "is_reddit_media_domain","is_video","link_flair_type","media_only","num_comments","num_crossposts",
          "retrieved_on","score","selftext","spoiler","stickied","subreddit","subreddit_subscribers",
          "subreddit_type","title","total_awards_received","url","edited","updated_utc","gilded",
          "upvote_ratio","author_id","approved_at_utc","author_cakeday","view_count","author_created_utc"]

all_data = []

directory = "REDACTED/Raw_Subreddit_Data"

# store data in one dataframe, adding a column for the source file
for file in os.listdir(directory):
    df = pd.read_csv(os.path.join(directory, file))
    df = df[fields]
    df["source_file"] = file

    all_data.append(df)
   
full_df = pd.concat(all_data)
full_df.to_csv("REDACTED/All_Subreddit_Data_20220207.csv", index = False)

# LOAD COMBINED DATA FILE TO GOOGLE CLOUD PLATFORM
# from google.cloud import storage
# from google.cloud import bigquery
from google.oauth2 import service_account
import pandas_gbq


json_credentials = service_account.Credentials.from_service_account_file("REDACTED/REDACTED.json")

schema = [{'name': 'author', 'type': 'STRING'}, {'name': 'created_utc', 'type': 'STRING'}, 
        {'name': 'domain', 'type': 'STRING'}, {'name': 'full_link', 'type': 'STRING'}, 
        {'name': 'id', 'type': 'STRING'}, {'name': 'is_crosspostable', 'type': 'STRING'}, 
        {'name': 'is_original_content', 'type': 'STRING'}, 
        {'name': 'is_reddit_media_domain', 'type': 'STRING'}, {'name': 'is_video', 'type': 'STRING'}, 
        {'name': 'link_flair_type', 'type': 'STRING'}, {'name': 'media_only', 'type': 'STRING'}, 
        {'name': 'num_comments', 'type': 'STRING'}, {'name': 'num_crossposts', 'type': 'STRING'}, 
        {'name': 'retrieved_on', 'type': 'STRING'}, {'name': 'score', 'type': 'STRING'}, 
        {'name': 'selftext', 'type': 'STRING'}, {'name': 'spoiler', 'type': 'STRING'}, 
        {'name': 'stickied', 'type': 'STRING'}, {'name': 'subreddit', 'type': 'STRING'}, 
        {'name': 'subreddit_subscribers', 'type': 'STRING'}, {'name': 'subreddit_type', 'type': 'STRING'}, 
        {'name': 'title', 'type': 'STRING'}, {'name': 'total_awards_received', 'type': 'STRING'}, 
        {'name': 'url', 'type': 'STRING'}, {'name': 'edited', 'type': 'STRING'}, 
        {'name': 'updated_utc', 'type': 'STRING'}, {'name': 'gilded', 'type': 'STRING'}, 
        {'name': 'upvote_ratio', 'type': 'STRING'}, {'name': 'author_id', 'type': 'STRING'}, 
        {'name': 'approved_at_utc', 'type': 'STRING'}, {'name': 'author_cakeday', 'type': 'STRING'}, 
        {'name': 'view_count', 'type': 'STRING'}, {'name': 'author_created_utc', 'type': 'STRING'}, 
        {'name': 'source_file', 'type': 'STRING'}]

df_gbq = pd.read_csv("REDACTED/All_Subreddit_Data_20220207.csv")
df_gbq = df_gbq.astype(str)


# In[106]:


df_gbq.to_gbq("SL_Intern_Project_2022.orig_subreddit_data_20220207", project_id = "REDACTED", 
              if_exists = "fail", table_schema = schema, progress_bar = True, credentials = json_credentials)

