#libraries required for the application
import streamlit as st
import pymongo
import pandas as pd
from pymongo import MongoClient
import sqlite3
import googleapiclient.discovery


#api key
api_key="AIzaSyBgksqny_siibIBM4jWgZisgDbeqdrsdPI"
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

def time_duration(t):
    a = pd.Timedelta(t)
    b = str(a).split()[-1]
    return b
#information about the channel by their channel id
def channel_information(channelID): 
    channel_data=[]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channelID)
    response = request.execute()
    channel=dict(channel_id=(response['items'][0]['id']),
                      channel_name=(response['items'][0]['snippet']['title']),
                      channel_description=(response['items'][0]['snippet']['description']),
                      subscribers=(response['items'][0]['statistics']['subscriberCount']),
                      video_count=(response['items'][0]['statistics']['videoCount']),
                      joined_date=(response['items'][0]['snippet']['publishedAt'][0:10]),
                      channel_views=(response['items'][0]['statistics']['viewCount']))
    channel_data.append(channel)
    return channel_data
#fetching all the video ids of the channel using channel_id
def videoId_information(channel_id):
    video_ids = []
    res = youtube.channels().list(id=channel_id, 
                                  part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        res = youtube.playlistItems().list( 
                                           part = 'snippet',
                                           playlistId = playlist_id, 
                                           maxResults = 50,
                                           pageToken = next_page_token).execute()
        
        for i in range(len(res['items'])):
            video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = res.get('nextPageToken')
        
        if next_page_token is None:
            break
    return video_ids
#fetching all the videos using video_id
def video_information(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(video_id = item['id'],
                        channel_id = item['snippet']['channelId'],
                        video_name = item['snippet']['title'],
                        thumbnail = item['snippet']['thumbnails']['default']['url'],
                        video_description = item['snippet']['description'],
                        published_Date = item['snippet']['publishedAt'],
                        video_duration = time_duration(item['contentDetails']['duration']),
                        video_views = item['statistics']['viewCount'],
                        video_likes = item['statistics'].get('likeCount'),
                        comments_count = item['statistics'].get('commentCount'),
                        favorite_Count = item['statistics']['favoriteCount'],
                        video_definition = item['contentDetails']['definition'])
            video_data.append(data)
    return video_data

#get comment information using video_id
def comment_information(video_ids):
    Comment_Information = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50)
            response5 = request.execute()
            for item in response5["items"]:
                comment_information = dict(
                    comment_id = item["snippet"]["topLevelComment"]["id"],
                    video_id = item["snippet"]["videoId"],
                    comment_text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                    comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    comment_published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                Comment_Information.append(comment_information)
    except:
        pass
    return Comment_Information




#inserting the data to mongodb compass
client=MongoClient('mongodb://localhost:27017/')
db=client["youtube"]

collection1 = db["channel_data"]
collection2 = db["video_data"]
collection3 = db["comment_data"]
def channel_details(channel_id):
    ch_details = channel_information(channel_id)
    vi_ids = videoId_information(channel_id)
    vi_details = video_information(vi_ids)
    com_details = comment_information(vi_ids)
    
        # Insert channel details into 'channel_data' collection
    if ch_details:
        collection1.insert_many(ch_details)
    else:
        print("Channel details list is empty.")

        # Insert video details into 'video_data' collection
    if vi_details:
        collection2.insert_many(vi_details)
    else:
        print("Video details list is empty.")

        # Insert comment details into 'comment_data' collection
    if com_details:
        collection3.insert_many(com_details)
    else:
        print("Comment details list is empty.")

    return "success"


#channel_details('UCX7NrBIGUezEJxDK3vCZa7Q')

sqlite_connection = sqlite3.connect('sample.db')
cursor = sqlite_connection.cursor()
#Defining sql table 
def convert_to_table():
    cursor.execute('DROP TABLE IF EXISTS channel_table')
    cursor.execute('DROP TABLE IF EXISTS video_table')
    cursor.execute('DROP TABLE IF EXISTS comment_table')

    # Create new channel_table
    cursor.execute('''
        CREATE TABLE channel_table (
            channel_id TEXT PRIMARY KEY,
            channel_name TEXT,
            channel_description TEXT,
            subscribers INTEGER,
            video_count INTEGER,
            joined_date TEXT,
            channel_views INTEGER
        )
    ''')

    # Create new video_table
    cursor.execute('''
        CREATE TABLE video_table (
            video_id TEXT PRIMARY KEY,
            channel_id TEXT,
            video_name TEXT,
            thumbnail TEXT,
            video_description TEXT,
            published_date TEXT,
            video_duration TEXT,
            video_views INTEGER,
            video_likes INTEGER,
            comments_count INTEGER,
            favorite_count INTEGER,
            video_definition TEXT,
            FOREIGN KEY (channel_id) REFERENCES channel_table (channel_id)
        )
    ''')

    # Create new comment_table
    cursor.execute('''
        CREATE TABLE comment_table (
            video_id TEXT,
            comment_id TEXT PRIMARY KEY,
            comment_text TEXT,
            comment_author TEXT,
            comment_published TEXT,
            FOREIGN KEY (video_id) REFERENCES video_table (video_id)
        )
    ''')

    # Insert values into channel_table
    channel_data = list(collection1.find())
    for channel in channel_data:
        cursor.execute('''
            INSERT INTO channel_table VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            channel['channel_id'],
            channel['channel_name'],
            channel['channel_description'],
            channel['subscribers'],
            channel['video_count'],
            channel['joined_date'],
            channel['channel_views']
        ))

    # Insert values into video_table
    video_data = list(collection2.find())
    for video in video_data:
        cursor.execute('''
            INSERT INTO video_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            video['video_id'],
            video['channel_id'],
            video['video_name'],
            video['thumbnail'],
            video['video_description'],
            video['published_Date'],
            video['video_duration'],
            video['video_views'],
            video['video_likes'],
            video['comments_count'],
            video['favorite_Count'],
            video['video_definition']
        ))

    # Insert values into comment_table
    comment_data = list(collection3.find())
    for comment in comment_data:
        cursor.execute('''
            INSERT INTO comment_table VALUES (?, ?, ?, ?, ?)
        ''', (
            comment['video_id'],
            comment['comment_id'],
            comment['comment_text'],
            comment['comment_author'],
            comment['comment_published']
        ))

    # Commit the changes 
    sqlite_connection.commit()
    return "uploaded to sql"

def show_channel_table():
    pd.set_option('display.max_colwidth', None)
    sql_query = "SELECT * FROM channel_table;"
    df = pd.read_sql_query(sql_query, sqlite_connection)
    st.dataframe(df) #display(df)
def show_video_table():
    pd.set_option('display.max_colwidth', None)
    sql_query = "SELECT * FROM video_table;"
    df = pd.read_sql_query(sql_query, sqlite_connection)
    st.dataframe(df)
def show_comment_table():
    pd.set_option('display.max_colwidth', None)
    sql_query = "SELECT * FROM comment_table;"
    df = pd.read_sql_query(sql_query, sqlite_connection)
    st.dataframe(df)



# streamlit code

st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
st.header("Description")
st.caption("The goal is to create a Streamlit application that will let users examine data from different YouTube channels. To view features including channel information, video details, and user interaction analytics, users must enter their YouTube channel ID. The software ought to enable users to collect data from up to ten distinct channels and store it in a MongoDB database. It should also have the ability to move specific channel data from MongoDB to a SQL database for further examination. With sophisticated features like table joins for an extensive view of channel information, the application should enable users to search and get data from the SQL database.")
    
channel_id=st.text_input("Channel ID")

if st.button("Fetch data to MongoDB"):
    db = client["youtube"]
    collection1 = db["channel_data"]
    ch_ids=[]
    for ch_data in collection1.find():
        Channel_ID=ch_data.get("channel_id")
        ch_ids.append(Channel_ID)
    if channel_id in ch_ids:
        st.success("Channel already exists in the database")
    else:
        upload=channel_details(channel_id)
        st.success(upload)

if st.button("Convert to SQL"):
    Table=convert_to_table()
    st.success(Table)
    
if st.button("Channel Table"):
    show_channel_table()
if st.button("Video Table"):
    show_video_table()
if st.button("Comment Table"):
    show_comment_table()


#sql queries
sqlite_connection = sqlite3.connect('sample.db')
cursor = sqlite_connection.cursor()

question=st.radio("Choose any queries",("1. What are the names of all the videos and their corresponding channels?",
                                   "2. Which channels have the most number of videos, and how many videos do they have?",
                                   "3. What are the top 10 most viewed videos and their respective channels?",
                                   "4. How many comments were made on each video, and what are their corresponding video names?",
                                   "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                   "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                   "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                   "8. What are the names of all the channels that have published videos in the year 2022?",
                                   "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                   "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

st.write(f"Selected Question: {question}")


sqlite_connection = sqlite3.connect('sample.db')
cursor = sqlite_connection.cursor()


if question==("1. What are the names of all the videos and their corresponding channels?"):
    #query 1
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT video_table.video_name, channel_table.channel_name FROM video_table JOIN channel_table ON video_table.channel_id = channel_table.channel_id;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.dataframe(df)

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
    #query 2
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT channel_name,video_count FROM channel_table ORDER BY video_count DESC;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    #query 3
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT video_table.video_name, channel_table.channel_name, video_table.video_views FROM video_table JOIN channel_table ON video_table.channel_id=channel_table.channel_id ORDER BY video_views DESC LIMIT 10;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
    #query 4
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT video_name,comments_count FROM video_table;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    #query 5
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT video_table.video_name, video_table.video_likes, channel_table.channel_name FROM video_table JOIN channel_table ON video_table.channel_id = channel_table.channel_id ORDER BY video_likes DESC LIMIT 5;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    #query 6
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT video_name, video_likes FROM video_table;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    #query 7
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT channel_name,channel_views FROM channel_table;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    #query 8
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT DISTINCT(channel_table.channel_name) FROM channel_table JOIN video_table ON channel_table.channel_id = video_table.channel_id WHERE video_table.published_date LIKE '2022%';"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    #query 9
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT AVG(duration_seconds) AS average_duration_seconds FROM (SELECT CAST(SUBSTR(video_duration, 1, 2) AS INTEGER) * 3600 + CAST(SUBSTR(video_duration, 4, 2) AS INTEGER) * 60 + CAST(SUBSTR(video_duration, 7, 2) AS INTEGER) AS duration_seconds FROM video_table) AS durations;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    #query 10
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    sql_query="SELECT channel_table.channel_name, video_table.video_name,video_table.comments_count FROM channel_table JOIN video_table ON channel_table.channel_id=video_table.channel_id ORDER BY video_table.comments_count DESC LIMIT 10;"
    df=pd.read_sql_query(sql_query,sqlite_connection)
    #display(df)
    st.write(df)
