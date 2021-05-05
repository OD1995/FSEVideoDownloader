import pyodbc
import logging
import os
from datetime import datetime
import urllib.parse as urlparse
from urllib.parse import parse_qs
from dateutil.parser import isoparse
import requests

def update_VideosToDownload(
    rowID,
    newStatus
):
    logging.info("update_VideosToDownload running")
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    updateStatement = f"""
UPDATE VideosToDownload
SET [Status] = '{newStatus}' AND [LastStatusUpdate] = '{currentTime}'
WHERE [RowID] = {rowID}
    """
    logging.info(f"updateStatement: {updateStatement}")

def run_sql_query(query):
    connectionString = get_connection_string()
    logging.info(f"connectionString: {connectionString}")
    ## Execute query
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            logging.info("About to execute query")
            cursor.execute(query)
            logging.info("Query executed")

def get_connection_string():
    ## Get information used to create connection string
    username = 'matt.shepherd'
    password = os.getenv("sqlPassword")
    driver = '{ODBC Driver 17 for SQL Server}'
    server = os.getenv("sqlServer")
    database = 'AzureCognitive'
    ## Create connection string
    return f'DRIVER={driver};SERVER={server};PORT=1433;DATABASE={database};UID={username};PWD={password}'

def get_video_name(
    vidURL
):
    if "youtube.com" in vidURL.lower():
        ## Extract the video ID
        parsed = urlparse.urlparse(vidURL)
        videoID = parse_qs(parsed.query)['v'][0]
        ## Use the YouTube API to get the video's title
        videoTitle,publishDate = get_yt_vid_info(videoID)
        ## If it's an NZ Cricket video, make some adjustments
        nzc = [
            "super smash",
            "blackcaps",
            "white ferns"
        ]
        if any([x in videoTitle.lower() for x in nzc]):
            ## Remove things
            rt = [
                "FULL LIVE MATCH ",
                "LIVE MATCH ",
                "LIVE COVERAGE | ",
            ]
            for r in rt:
                videoTitle = videoTitle.replace(r,"")
            ## Add date at the start
            dateString = publishDate.strftime("%Y%m%d")
            videoTitle = f"{dateString} - {videoTitle}"

    elif "twitch.com" in vidURL.lower():
        pass
    else:
        raise ValueError("URL is from neither YouTube nor Twitch")

    return videoTitle

def get_yt_vid_info(
    videoID
):
    req = requests.get(
        url="https://www.googleapis.com/youtube/v3/videos",
        params={
            'id': videoID,
            'part': 'snippet',
            'key': os.getenv("youtube_api_key")
        })
    vidName = req.json()['items'][0]['snippet']['title']
    publishDate = isoparse(req.json()['items'][0]['snippet']['publishedAt'])
    return vidName,publishDate