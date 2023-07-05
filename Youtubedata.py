import googleapiclient.discovery
import requests
import json
import pymongo

import pandas as pd
import streamlit as st
st.set_page_config(layout='wide')
st.title(":red[Youtube data harvesting]")
col1,col2 = st.columns(2)
with col1:
    st.header(':violet[Data Collection]')
    channel_id = st.text_input("Enter channel_id")
                               # ['UCNU_lfiiWBdtULKOw6X0Dig',# Krish Naiyak,
                               #  'UCYO_jab_esuFRV4b17AJtAw',#3Blue1Brown,
                               #  'UCLLw7jmFsvfIVaUFsLs8mlQ', # Luke Barousse,
                               #   'UCiT9RITQ9PW6BhXK0y2jaeg', # Ken Jee,
                               #   'UC7cs8q-gJRlGwj4A8OmCmXg', # Alex the analyst\,
                               #    'UC2UXDak6o7rBm23k3Vv5dww',# Tina Huang,
                               #    'UCvjgXvBlbQiydffZU7m1_aw',#The Coding Train,
                               #     'UCFp1vaKzpfvoGai0vE5VJ0w',#Guy in a cube,
                               #      'UCCezIgC97PvUuR4_gbFUs5g',#Corey Shafer,
                               #        ])
    get_data = st.button('store data about the channel')

if "get_state" not in st.session_state:
    st.session_state.get_state = False
if get_data or st.session_state.get_state:
    st.session_state.get_state = True

api_service_name = 'youtube'
api_version = 'v3'
api_key = 'AIzaSyC8Z0k9-Tbz1SoR62AOTpNOytD_dWRuOuM'
channel_id = 'UC7cs8q-gJRlGwj4A8OmCmXg'

base_url = "https://www.googleapis.com/youtube/v3/"
channel_params = {
    "part": "snippet,statistics,contentDetails",
    "id": channel_id,
    "key": api_key
}

channel_response = requests.get(base_url + "channels", params=channel_params)
channel_data = channel_response.json()

channel_name = channel_data["items"][0]["snippet"]["title"]
channel_description = channel_data["items"][0]["snippet"]["description"]
subscription_count = channel_data["items"][0]["statistics"]["subscriberCount"]
channel_views = channel_data["items"][0]["statistics"]["viewCount"]
playlist_ids = channel_data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

playlist_params = {
    "part": "snippet",
    "playlistId": playlist_ids,
    "key": api_key,
    "maxResults": 50
}

playlist_response = requests.get(base_url + "playlistItems", params=playlist_params)
playlist_data = playlist_response.json()

video_info = {}
for item in playlist_data["items"]:
    video_id = item["snippet"]["resourceId"]["videoId"]
    video_name = item["snippet"]["title"]
    video_params = {
        "part": "snippet,statistics,contentDetails",
        "id": video_id,
        "key": api_key
    }

    video_response = requests.get(base_url + "videos", params=video_params)
    video_data = video_response.json()
    # Extract the video information
    video_description = video_data["items"][0]["snippet"]["description"]
    tags = video_data["items"][0]["snippet"]["tags"]
    published_at = video_data["items"][0]["snippet"]["publishedAt"]
    view_count = video_data["items"][0]["statistics"]["viewCount"]
    like_count = video_data["items"][0]["statistics"]["likeCount"]
    favorite_count = video_data["items"][0]["statistics"]["favoriteCount"]
    comment_count = video_data["items"][0]["statistics"]["commentCount"]
    duration = video_data["items"][0]["contentDetails"]["duration"]
    thumbnail = video_data["items"][0]["snippet"]["thumbnails"]["default"]["url"]
    caption_status = video_data["items"][0]["contentDetails"]["caption"]

    comment_params = {
        "part": "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults": 50  # Change this as per your need
    }

    comment_response = requests.get(base_url + "commentThreads", params=comment_params)
    comment_data = comment_response.json()
    comments = {}

    for comment in comment_data["items"]:
        comment_id =comment["id"]
        comment_text = comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
        comment_author = comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
        comment_published_at = comment["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
        comments[comment_id] = {
            "Comment_Id": comment_id,
            "Comment_Text": comment_text,
            "Comment_Author": comment_author,
            "Comment_PublishedAt": comment_published_at
        }

    # Store the video information in a dictionary with video ID as key
    video_info[video_id] = {
        "Video_Id": video_id,
        "Video_Name": video_name,
        "Video_Description": video_description,
        "Tags": tags,
        "PublishedAt": published_at,
        "View_Count": view_count,
        "Like_Count": like_count,
        "Favorite_Count": favorite_count,
        "Comment_Count": comment_count,
        "Duration": duration,
        "Thumbnail": thumbnail,
        "Caption_Status": caption_status,
        "Comments": comments
    }
# Store the channel information in a dictionary with channel name as key
channel_info = {
    channel_name: {
        "Channel_Name": channel_name,
        "Channel_Id": channel_id,
        "Subscription_Count": subscription_count,
        "Channel_Views": channel_views,
        "Channel_Description": channel_description,
        "Playlist_Id": playlist_ids
    }
}
# Merge the channel and video information into one dictionary
youtube_data = {**channel_info, **video_info}
# Convert the dictionary to JSON format
youtube_json = json.dumps(youtube_data, indent=4)
#print(youtube_json)

#Import data to pymongo
connection = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = connection['Youtube_Db']
collection = mydb['Youtube_data']
final_data= {'Channel_Name': channel_name,
            "Channel_data": youtube_data}
upload = collection.insert_one(final_data)
connection.close()

with col2:
    st.header(':blue[Data migrate zone]')
    connection = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = connection['Youtube_Db']
    collection = mydb['Youtube_data']
    document_names= []
    for document in collection.find():
        document_names.append(document['Channel_Name'])
    document_name = st.selectbox('**Select Channel name**', options=document_names, key='document_names')
    Migrate = st.button('Migrate to MySQL')

    if 'migrate_sql' not in st.session_state:
        st.session_state_migrate_sql = False
    if Migrate or st.session_state_migrate_sql:
        st.session_state_migrate_sql = True

    result = collection.find_one({"Channel_Name": document_name})
    connection.close()




