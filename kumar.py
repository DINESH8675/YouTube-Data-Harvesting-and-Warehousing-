import googleapiclient.discovery
import pandas as pd
import psycopg2
import pymongo
import streamlit as st
import plotly.express as px
import datetime
from googleapiclient.discovery import build
import isodate

st.set_page_config(page_title="youtube  data harvesting and warehousing",page_icon="🧊",layout="wide")
st.header('Youtube Data Harvesting and Warehousing')

api='AIzaSyA6eqzLzcWDYGFU9UVHQsFzfNczaQDg0ZM'
channel_id=st.text_input("enter the channel id")
st.button('Enter')

api_service_name="youtube"
api_version="v3"
youtube=build(api_service_name,api_version,developerKey=api)

def get_channel_details(youtube, channel_id):
    request = youtube.channels().list(
        part='contentDetails, snippet, statistics, status',
        id=channel_id)
    response = request.execute()

    data = {'channel_name': response['items'][0]['snippet']['title'],
            'channel_id': response['items'][0]['id'],
            'subscription_count': response['items'][0]['statistics']['subscriberCount'],
            'channel_views': response['items'][0]['statistics']['viewCount'],
            'channel_description': response['items'][0]['snippet']['description'],
            'upload_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'channel_video_count':response['items'][0]['statistics']['videoCount'],
            'country': response['items'][0]['snippet']['country']}
    return data


channeldata=get_channel_details(youtube,channel_id)
upload_id=channeldata["upload_id"]

def get_total_playlists(youtube, channel_id,upload_id):
    all=[]
    request = youtube.playlists().list(
        part="snippet,contentDetails,status",
        channelId=channel_id,
        maxResults=50)
    response = request.execute()

    

    for i in range(0, len(response['items'])):
        data =dict (playlist_id= response['items'][i]['id'],
                    playlist_name=response['items'][i]['snippet']['title'],
                    channel_id= channel_id,
                    upload_id = upload_id
                    )
        all.append(data)     
        
    next_page_token = response.get('nextPageToken')

    next_page = True
    while next_page:
        if next_page_token is None:
            next_page = False
        else:
            request = youtube.playlists().list(
                part="snippet,contentDetails,status",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = dict(playlist_id =  response['items'][i]['id'],
                        playlist_name=response['items'][i]['snippet']['title'],
                        channel_id= channel_id,
                        upload_id = upload_id)
                all.append(data)
                 
            next_page_token = response.get('nextPageToken')

    return all

def get_total_video_ids(youtube,upload_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=upload_id,
        maxResults=50)
    response = request.execute()

    list_video_ids = []

    for i in range(0, len(response['items'])):
        data = response['items'][i]['contentDetails']['videoId']
        list_video_ids.append(data)
    next_page_token = response.get('nextPageToken')

    next_page = True
    while next_page:
        if next_page_token is None:
            next_page= False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=upload_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = response['items'][i]['contentDetails']['videoId']
                list_video_ids.append(data)
            next_page_token = response.get('nextPageToken')

    return list_video_ids

video_id=get_total_video_ids(youtube,upload_id)

def get_video_details(youtube,video_id,upload_id):
   
     all=[]
     
     for i in video_id:
       video_data = get_video_details1(youtube, i, upload_id)
       all.append(video_data)
     return all
def duration(duration):
    duration_obj = isodate.parse_duration(duration)
    hours = duration_obj.total_seconds() // 3600
    minutes = (duration_obj.total_seconds() % 3600) // 60
    seconds = duration_obj.total_seconds() % 60
    formatted_duration = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return formatted_duration

def get_video_details1(youtube,video_id,upload_id):

     
     request = youtube.videos().list(
             part='contentDetails, snippet, statistics',
             id=video_id)
     response = request.execute()
     

     data = {'video_id': response['items'][0]['id'],
            'playlist_id': upload_id,
            'video_name': response['items'][0]['snippet']['title'],
            'video_description': response['items'][0]['snippet']['description'],
            'tags': response['items'][0]['snippet'].get('tags', []),
            'published_date': response['items'][0]['snippet']['publishedAt'][0:10],
            'published_time': response['items'][0]['snippet']['publishedAt'][11:19],
            'view_count': response['items'][0]['statistics']['viewCount'],
            'like_count': response['items'][0]['statistics'].get('likeCount', 0),
            'favourite_count': response['items'][0]['statistics']['favoriteCount'],
            'comment_count': response['items'][0]['statistics'].get('commentCount', 0),
            'duration': duration(response['items'][0]['contentDetails']['duration']),
            'thumbnail': response['items'][0]['snippet']['thumbnails']['default']['url'],
            'channel_name': response['items'][0]["snippet"]["channelTitle"],
            'caption_status':response['items'][0]['contentDetails']['caption']}
      

     return data

def get_comments_details(youtube, video_id):
  
  for video_id in video_id:
    request = youtube.commentThreads().list(
        part='id, snippet,replies',
        videoId=video_id,
        maxResults=100)
    response = request.execute()

    list_comments = {}
    c = 1
    data={}
    for i in range(0, len(response['items'])):
        data = {'comment_id': response['items'][i]['id'],
                'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][
                                          0:10],
                'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][
                                          11:19],
                'video_id': video_id}
        c1 = 'comment_id_' + str(c)
        list_comments[c1] = data
        c += 1
    return list_comments 


def storetomongodb():
  client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
  db = client["youtubeProject"]
  col = db["channelDetails"]   
  col.insert_one(final)

def create_table():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR', port=5432,database='youtube1')
    dinesh = kumar.cursor()
    dinesh.execute("create table if not exists channel(\
                                        channel_id 			varchar(255) primary key,\
                                        channel_name		varchar(255),\
                                        subscription_count	int,\
                                        channel_views		int,\
                                        channel_description	text,\
                                        upload_id           varchar(255),\
                                        channel_video_count  int,\
                                        country				varchar(255))")

    dinesh.execute("create table if not exists playlist(\
                                        playlist_id		varchar(255) primary key,\
                                        playlist_name	varchar(255),\
                                        channel_id		varchar(255),\
                                        upload_id       varchar(255))")

    dinesh.execute("create table if not exists video(\
                                        video_id			varchar(255) primary key,\
                                        playlist_id         varchar(255),\
                                        video_name			varchar(255),\
                                        video_description	text,\
                                        tags				text,\
                                        published_date		date,\
                                        published_time		time,\
                                        view_count			int,\
                                        like_count			int,\
                                        favourite_count		int,\
                                        comment_count		int,\
                                        duration			time,\
                                        thumbnail			varchar(255),\
                                        channel_name		varchar(255),\
                                        caption_status		varchar(255))")

    dinesh.execute("create table if not exists comment(\
                                        comment_id				varchar(255) primary key,\
                                        comment_text			text,\
                                        comment_author			varchar(255),\
                                        comment_published_date	date,\
                                        comment_published_time	time,\
                                        video_id				varchar(255))")  
    kumar.commit()

def mongodb():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col = db["channelDetails"]
    


def sql_channel():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col = db["channelDetails"]
    
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    data = []
    for i in col.find({}, {'_id': 0, 'channeldetails': 1}):
        data.append(i['channeldetails'])

    channel = pd.DataFrame(data)
    channel = channel.reindex(columns=['channel_id', 'channel_name', 'subscription_count', 'channel_views',
                                       'channel_description', 'upload_id','channel_video_count','country'])
    channel['subscription_count'] = pd.to_numeric(channel['subscription_count'])
    channel['channel_views'] = pd.to_numeric(channel['channel_views'])
    channel['channel_video_count']= pd.to_numeric(channel['channel_video_count'])
    return channel


def sql_playlists():
    client = pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col = db["channelDetails"]
    
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    
    data = []
    for i in col.find({}, {'_id': 0, 'playlistdetails': 1}):
        data.append(i['playlistdetails'])
 
    playlists = pd.DataFrame(data[0])
    playlists = playlists.reindex(columns=['playlist_id', 'playlist_name', 'channel_id','upload_id'])
    return playlists

 

def sql_videos():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col = db["channelDetails"]

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0,"vodeodetails":1}):
        data.append(i["vodeodetails"])

    videos = pd.DataFrame(data[0])
    videos = videos.reindex(
        columns=['video_id','playlist_id' ,'video_name', 'video_description', 'tags', 'published_date', 'published_time',
                 'view_count', 'like_count', 'favourite_count', 'comment_count', 'duration', 'thumbnail','channel_name',
                 'caption_status'])

    videos['published_date'] = pd.to_datetime(videos['published_date']).dt.date
    videos['published_time'] = pd.to_datetime(videos['published_time'], format='%H:%M:%S').dt.time
    videos['view_count'] = pd.to_numeric(videos['view_count'])
    videos['like_count'] = pd.to_numeric(videos['like_count'])
    videos['favourite_count'] = pd.to_numeric(videos['favourite_count'])
    videos['comment_count'] = pd.to_numeric(videos['comment_count'])
    
    return videos

 

def sql_comments():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col = db["channelDetails"]
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    data = []
    for i in col.find({}, {'_id': 0, "commentsdetails":1}):
        data.append(i["commentsdetails"])

    
    comments = pd.DataFrame(data[0])
    comments = comments.reindex(columns=['comment_id', 'comment_text', 'comment_author',
                                         'comment_published_date', 'comment_published_time', 'video_id'])
    comments['comment_published_date'] = pd.to_datetime(comments['comment_published_date']).dt.date
    comments['comment_published_time'] = pd.to_datetime(comments['comment_published_time'], format='%H:%M:%S').dt.time
    return comments



def sql_final():
   create_table()
   client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
   db = client["youtubeProject"]
   col = db["channelDetails"]
   
   channel = sql_channel()
   playlists = sql_playlists()
   videos = sql_videos()
   comments = sql_comments()
   pd.set_option('display.max_rows', None)
   pd.set_option('display.max_columns', None)
   kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432, database='youtube1')
   dinesh= kumar.cursor()

   dinesh.executemany("""insert into channel(channel_id, channel_name, subscription_count, channel_views,\
                                                    channel_description,upload_id,channel_video_count,country) values(%s,%s,%s,%s,%s,%s,%s,%s)""",
                                   channel.values.tolist())
   dinesh.executemany("""insert into playlist(playlist_id, playlist_name, channel_id ,upload_id)\
                                                    values(%s,%s,%s,%s)""", playlists.values.tolist())
   dinesh.executemany("""insert into video(video_id,playlist_id, video_name, video_description, tags, published_date,\
                                                    published_time, view_count, like_count, favourite_count, comment_count, duration, thumbnail,\
                                                    channel_name,caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                   videos.values.tolist())
   dinesh.executemany("""insert into comment(comment_id, comment_text, comment_author, comment_published_date,\
                                                    comment_published_time, video_id) values(%s,%s,%s,%s,%s,%s)""",
                                   comments.values.tolist())

   kumar.commit()
   st.success("Migrated Data Successfully to SQL Data Warehouse")
   kumar.close()

def dropdownlist():
     chanelname =[]
     client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
     for i in client["YoutubeProject"]["channel_Details"].find():
         chanelname.append(i["channeldetails"]["channel_name"])
     return(chanelname)


def question1():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name ,video_name from video")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 5000,column_config=({1:"Channel Name",2:"Video Name"}))


def question2():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name ,channel_video_count from channel order by channel_video_count desc limit 5 ")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 5000,column_config=({1:"Channal_name",2:"channel_video_count"}))


def question3():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name ,video_name from video order by view_count asc limit 10  ")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 1000,column_config=({1:"channel_name",2:"video_name"}))
  


def question4():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select comment_count, video_name from video order by comment_count asc")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 1000,column_config=({1:"comment_count",2:"video_name"}))
  
   


def question5():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select video_name as Video_name,channel_name as Channel_name, like_count from video order by like_count asc")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 1000,column_config=({1:"Video_name",2:"Channel_name",3:"like_count"}))


def question6():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select  video_name ,like_count from video order by like_count desc limit 15 ")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 1000,column_config=({1:"Video_name",2:"Channel_name",3:"like_count"}))


def question7():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name, channel_views from channel")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width=1000,column_config=({1:"Video_name",2:"Channel_name",3:"like_count"}))
  


def question8():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute('select distinct channel_name from video where extract(year from published_date) = 2022;')
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 1000,column_config=({1:"Channel_name"}))


def question9():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name , cast(avg(duration) as varchar)from video group by channel_name")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 1000,column_config=({1:"Channel_name",2:"avg"}))
   


def question10():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube1')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name as channel,video_name as video,comment_count from video order by comment_count desc limit 10")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width= 1000,column_config=({1:"Channel",2:"video",3:"comment_count"}))


def sql_quries():
    st.subheader('Select the Query below')
    Q1="What are the names of all the videos and their corresponding channels?"
    Q2="Which channels have the most number of videos, and how many videos do they have?"
    Q3="What are the top 10 most viewed videos and their respective channels?"
    Q4="How many comments were made on each video, and what are theircorresponding video names?"
    Q5="Which videos have the highest number of likes, and what are their corresponding channel names?"
    Q6="What is the total number of likes and dislikes for each video, and what are their corresponding video names?"
    Q7="What is the total number of views for each channel, and what are their corresponding channel names?"
    Q8="What are the names of all the channels that have published videos in the year 2022?"
    Q9="What is the average duration of all videos in each channel, and what are their corresponding channel names?"
    Q10="Which videos have the highest number of comments, and what are their corresponding channel names?"
    sql_query_selection = st.selectbox("",['Select One', Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10])
    if sql_query_selection== Q1:
        question1()
    elif sql_query_selection == Q2:
        question2()
    elif sql_query_selection == Q3:
        question3()
    elif sql_query_selection== Q4:
        question4()
    elif sql_query_selection == Q5:
        question5()
    elif sql_query_selection == Q6:
        question6()
    elif sql_query_selection== Q7:
        question7()
    elif sql_query_selection == Q8:
        question8()
    elif sql_query_selection == Q9:
        question9()
    elif sql_query_selection == Q10:
        question10()


st.code( 'data extract from the YouTube API')
st.code('Store data to MongoDB')
st.code(' Migrating data from mongodb to  SQL data warehouse')
st.code('all sql Queries')

list_options = ['data extract from the YouTube API', 'Store data to MongoDB',
                ' Migrating data from mongodb to  SQL data warehouse',  'all sql Queries']
option = st.selectbox(" ",list_options)





if option== 'data extract from the YouTube API':
       enter=st.button("press")
       if enter :
           final ={"channeldetails":get_channel_details(youtube, channel_id),"playlistdetails":get_total_playlists(youtube, channel_id, upload_id),"vodeodetails":get_video_details(youtube,video_id,upload_id),"commentsdetails":get_comments_details(youtube, video_id)}
           a=final
           st.write(a)
      
elif option=='Store data to MongoDB':
    enter=st.button("press")
    if enter :
      final ={"channeldetails":get_channel_details(youtube, channel_id),"playlistdetails":get_total_playlists(youtube, channel_id, upload_id),"vodeodetails":get_video_details(youtube,video_id,upload_id),"commentsdetails":get_comments_details(youtube, video_id)}
      storetomongodb()

elif option == ' Migrating data from mongodb to  SQL data warehouse':
    enter=st.button("press")
    if enter :
       sql_final()

elif option =='all sql Queries':
   sql_quries()
