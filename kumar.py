
import pandas as pd
import psycopg2
import pymongo
import streamlit as st
from googleapiclient.discovery import build
import isodate

st.set_page_config(page_title='Youtube data harveting and Warehousing', page_icon=":youtube:", layout="wide")
st.title(":rainbow[YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit]")

api='AIzaSyDzRh7DntBphjmcvuPSTRNcdjW5z_Ep1EA'
api_service_name="youtube"
api_version="v3"
youtube=build(api_service_name,api_version,developerKey=api)

#---DATA SCRAPING FROM YOUTUBE USING YOUTUBE APIKEY----

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
            'playlist_id': response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
            'channel_video_count':response['items'][0]['statistics']['videoCount']}
    return data

def get_total_playlists(youtube, channel_id):
    request = youtube.playlists().list(
        part="snippet,contentDetails,status",
        channelId=channel_id,
        maxResults=50)
    response = request.execute()
    all_data=[]
    for i in range(0, len(response['items'])):
        data =dict (playlist_id= response['items'][i]['id'],
                    playlist_name=response['items'][i]['snippet']['title'],
                    channel_id= channel_id
                    )
        all_data.append(data)
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
                        channel_id= channel_id
                            )
                all_data.append(data)
            
                 
            next_page_token = response.get('nextPageToken')

    return all_data

def get_total_video_ids(youtube,playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
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
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(0, len(response['items'])):
                data = response['items'][i]['contentDetails']['videoId']
                list_video_ids.append(data)
            next_page_token = response.get('nextPageToken')

    return list_video_ids

def duration(duration):
    duration_obj = isodate.parse_duration(duration)
    hours = duration_obj.total_seconds() // 3600
    minutes = (duration_obj.total_seconds() % 3600) // 60
    seconds = duration_obj.total_seconds() % 60
    format= f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return format

def get_video_details(youtube,video_id):

     
     request = youtube.videos().list(
             part='contentDetails, snippet, statistics',
             id=video_id)
     response = request.execute()
     
     
     data = {'video_id': response['items'][0]['id'],
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
  all_comment=[]
  
  request = youtube.commentThreads().list(
        part='id, snippet,replies',
        videoId=video_id,
        maxResults=50)  
  response = request.execute()

    
  for i in range(0, len(response['items'])):
        data = {'comment_id': response['items'][i]['id'],
                'comment_text': response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                'comment_author': response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                'comment_published_date': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][
                                          0:10],
                'comment_published_time': response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'][
                                          11:19],
                 'video_id': video_id}
        all_comment.append(data)
 
  return all_comment
    
#-----DATA STORE TO MONGODB-----#

client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
db = client["youtubeProject"]

def channel_Details():
  channel=get_channel_details(youtube, channel_id)
  col1=db["channels"]
  col1.insert_one(channel)
  playlist=get_total_playlists(youtube, channel_id)
  col2=db["playlists"]
  for i in playlist:
     col2. insert_one(i)
  playlist=channel.get('playlist_id')
  videos=get_total_video_ids(youtube,playlist)
  for i in videos:
     d=get_video_details(youtube,i)
     col3=db["videos"]
     col3.insert_one(d)
     comment=get_comments_details(youtube,i)
     for i in comment:
        col4=db["comments"]
        col4.insert_one(i)

#------sample dataframe for mongo db data details ---

def channel_data():
  client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
  db = client["youtubeProject"]
  col1=db["channels"]
  detail=[]
  for i in col1.find({},{'_id':0}):
    detail.append(i)
  st.dataframe(detail)

def playlist_data():
     col2=db["playlists"]
     detail=[]
     for i in col2.find({},{'_id':0}):
       detail.append(i)
     st.dataframe(detail)


def video_data():
    col3=db["videos"]
    detail=[]
    for i in col3.find({},{'_id':0}):
       detail.append(i)
    st.dataframe(detail)
    
def comment_data():
    col4=db["comments"]
    detail=[]
    for i in col4.find({},{'_id':0}):
       detail.append(i)
    st.dataframe(detail)

#-----MIGRATING TO SQL DATABASE----

def create_channel_table():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col1=db["channels"]
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR', port=5432,database='youtube4')
    dinesh = kumar.cursor()
    dinesh.execute("create table if not exists channel(\
                                        channel_name		varchar(255),\
                                        channel_id 			varchar(255) primary key,\
                                        subscription_count	int,\
                                         channel_views		int,\
                                        channel_description	text,\
                                        playlist_id           varchar(255),\
                                        channel_video_count  int)")
                                       
    kumar.commit()

    try:
      data = []
      for i in col1.find({}, {'_id': 0}):
        data.append(i)
      channel = pd.DataFrame(data)
      dinesh.executemany("""insert into channel( channel_name,channel_id, subscription_count, channel_views,\
                                                    channel_description,playlist_id,channel_video_count) values(%s,%s,%s,%s,%s,%s,%s)""",
                        channel.values.tolist())                            
  
      kumar.commit()             
    except: 
      kumar.rollback()
   

def create_playlist_table():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col2=db["playlists"]
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR', port=5432,database='youtube4')
    dinesh = kumar.cursor()
    dinesh.execute("create table if not exists playlist(\
                                        playlist_id		varchar(255) primary key,\
                                        playlist_name	varchar(255),\
                                        channel_id		varchar(255))")
                                       
    kumar.commit()
     
    
    try:
        data = []
        for i in col2.find({}, {'_id': 0 }):
           data.append(i)
 
        playlist = pd.DataFrame(data)
        dinesh.executemany("""insert into playlist(playlist_id, playlist_name, channel_id )\
                                                    values(%s,%s,%s)""", 
                                        playlist.values.tolist())
        
    
        kumar.commit()
    except:
        kumar.rollback()



def create_videos_table():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col3=db["videos"]
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR', port=5432,database='youtube4')
    dinesh = kumar.cursor()
    dinesh.execute("create table if not exists video(\
                                        video_id			varchar primary key,\
                                        video_name			varchar,\
                                        video_description	text,\
                                        tags                text,\
                                        published_date		date,\
                                        published_time		time,\
                                        view_count			int,\
                                        like_count			int,\
                                        favourite_count		int,\
                                        comment_count		int,\
                                        duration			time,\
                                        thumbnail			varchar,\
                                        channel_name		varchar(255),\
                                        caption_status	varchar)") 
    kumar.commit()
    try:
       data  = []
       for i in col3.find({}, {'_id': 0}):
            data.append(i)

       videos = pd.DataFrame(data)
       dinesh.executemany("""insert into video(video_id,video_name, video_description, tags, published_date,\
                                                    published_time, view_count, like_count, favourite_count, comment_count, duration, thumbnail,\
                                                    channel_name,caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                                   videos.values.tolist())
       kumar.commit()
    except:
       kumar.rollback()
      
     


def create_comments_table():
    client= pymongo.MongoClient("mongodb+srv://r_dineshkumar:rdineshkumar@cluster0.e1g0htd.mongodb.net/?retryWrites=true&w=majority")
    db = client["youtubeProject"]
    col4=db["comments"]
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR', port=5432,database='youtube4')
    dinesh = kumar.cursor()
    dinesh.execute("create table if not exists comment(\
                                        comment_id				varchar(255) primary key,\
                                        comment_text			text,\
                                        comment_author			varchar(255),\
                                        comment_published_date	date,\
                                        comment_published_time	time,\
                                        video_id				varchar(255))") 
    kumar.commit() 
    try:
        data = []
        for i in col4.find({}, {'_id': 0}):
          data.append(i)

    
        comments = pd.DataFrame(data)
        dinesh.executemany("""insert into comment(comment_id, comment_text, comment_author, comment_published_date,\
                                                    comment_published_time, video_id) values(%s,%s,%s,%s,%s,%s)""",
                                   comments.values.tolist())
        kumar.commit()
    except:
        kumar.rollback()
 

# -------- 10 SQL QURIES-------#
def question1():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name ,video_name from video")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"Channel Name",2:"Video Name"}))

def question2():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name ,channel_video_count from channel order by channel_video_count desc limit 5 ")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"Channal_name",2:"channel_video_count"}))



def question3():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name ,video_name from video order by view_count asc limit 10  ")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"channel_name",2:"video_name"}))
  

def question4():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select comment_count, video_name from video order by comment_count desc")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"comment_count",2:"video_name"}))
  
  


def question5():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select video_name as Video_name,channel_name as Channel_name, like_count from video order by like_count asc")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    pd.set_option("display.max_columns",None)
    st.write(pd.DataFrame(a,columns=["Video_name","Channel_name","like_count"]))


def question6():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select  video_name ,like_count from video order by like_count desc limit 15 ")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,width=3000,column_config=({1:"Video_name",2:"like_count"}))

  


def question7():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name, channel_views from channel")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"Channel_name",2:"view_count"}))
  


def question8():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute('select distinct channel_name from video where extract(year from published_date) = 2022;')
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"Channel_name"}))


def question9():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name , cast(avg(duration) as varchar)from video group by channel_name")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"Channel_name",2:"avg_duration"}))


def question10():
    kumar = psycopg2.connect(host='localhost', user='postgres', password='DINESHKUMAR',port=5432,database='youtube4')
    dinesh= kumar.cursor()
    dinesh.execute("select channel_name as channel,video_name as video,comment_count from video order by comment_count desc limit 10")
    s = dinesh.fetchall()
    a=[]
    for i in s:
     a.append(i)
    st.dataframe(a,column_config=({1:"Channel_name",2:"video_name",3:"comment_count"}))






try:
    channel_id=st.text_input("enter the channel id")
    st.subheader(":blue[Retrieving data to store datalake and Migrating the data to  SQL Warehouse]:")
    option=st.selectbox(':violet[option]:',('selectone','Youtube data scraping and Data store to mongodb','Migrated to SQL Warehousing'))
    if  option =='Youtube data scraping and Data store to mongodb':
         channel_Details()

    elif option=='Migrated to SQL Warehousing':
        create_channel_table()
        create_playlist_table()
        create_videos_table()
        create_comments_table()
        st.success("succesfully migrated to sql warehouse")
except KeyError:
   pass

st.subheader(":red[Details In Dataframe]: ")
select=st.selectbox(":orange[select one to explore the details in dataframe format]:",("select anyone","1.channel_dataframe",
                   "2.playlist_dataframe","3.video_dataframe","4.comment_dataframe" ))                                                                  

if select=="select anyone":
   st.write("----") 
elif select=="1.channel_dataframe":
   channel_data()
   
elif select== "2.playlist_dataframe":
    playlist_data()

elif select=="3.video_dataframe":
   video_data()

elif select=="4.comment_dataframe":
   comment_data()

st.subheader(":rainbow[Analysis  The Data]:")
sql_quries=st.selectbox(':blue[Select the below quries]:',
    ("select one",
    "Q1=What are the names of all the videos and their corresponding channels?",
    "Q2=Which channels have the most number of videos, and how many videos do they have?",
    "Q3=What are the top 10 most viewed videos and their respective channels?",
    "Q4=How many comments were made on each video, and what are theircorresponding video names?",
    "Q5=Which videos have the highest number of likes, and what are their corresponding channel names?",
    "Q6=What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
    "Q7=What is the total number of views for each channel, and what are their corresponding channel names?",
    "Q8=What are the names of all the channels that have published videos in the year 2022?",
    "Q9=What is the average duration of all videos in each channel, and what are their corresponding channel names?",
    "Q10=Which videos have the highest number of comments, and what are their corresponding channel names?")) 
if sql_quries=="selectone":
    st.write(" ---")
elif sql_quries=="Q1=What are the names of all the videos and their corresponding channels?":
     question1()
elif sql_quries=="Q2=Which channels have the most number of videos, and how many videos do they have?":
   question2()
elif sql_quries=="Q3=What are the top 10 most viewed videos and their respective channels?":
   question3()
elif  sql_quries== "Q4=How many comments were made on each video, and what are theircorresponding video names?":
   question4()
elif sql_quries=="Q5=Which videos have the highest number of likes, and what are their corresponding channel names?":
   question5()
elif sql_quries== "Q6=What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
   question6()
elif sql_quries=="Q7=What is the total number of views for each channel, and what are their corresponding channel names?":
   question7()
elif sql_quries== "Q8=What are the names of all the channels that have published videos in the year 2022?":
   question8()
elif sql_quries==  "Q9=What is the average duration of all videos in each channel, and what are their corresponding channel names?":
   question9()
elif sql_quries== "Q10=Which videos have the highest number of comments, and what are their corresponding channel names?":
   question10()

