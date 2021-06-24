import pyodbc
import logging
import os
from datetime import datetime
import urllib.parse as urlparse
from urllib.parse import parse_qs
from dateutil.parser import isoparse
import requests
import twitch
from time import sleep

def update_VideosToDownload(
    rowID,
    newStatus
):
    logging.info("update_VideosToDownload running")
    currentTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    updateStatement = f"""
UPDATE VideosToDownload
SET [Status] = '{newStatus}', [LastStatusUpdate] = '{currentTime}'
WHERE [RowID] = {rowID}
    """
    logging.info(f"updateStatement: {updateStatement}")
    run_sql_query(updateStatement)

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
    if "youtube" in vidURL.lower():
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

    elif "twitch" in vidURL.lower():
        ## Extract the video ID
        videoID = urlparse.urlparse(vidURL).path.split("/")[-1]
        ## Get video name
        helix = twitch.Helix(
            client_id=os.getenv("twitch_clientID"),
            client_secret=os.getenv("twitch_clientsecret")
        )
        videoTitle = helix.video(videoID).title

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

def add_row_to_AzureBlobVideos(
    vidName : str,
    sport : str,
    endpointID : str,
    multipleVideoEvent : bool,
    samplingProportion : float,
    audioTranscript : bool,
    databaseID : str
):
    ## List of AzureBlobVideos columns
    columnList = [
        # 'VideoID', - auto incrementing
        'VideoName',
        'Event',
        'Sport',
        'EndpointID',
        'MultipleVideoEvent',
        'SamplingProportion',
        'AudioTranscript',
        'DatabaseID'
    ]
    ## Same as above, in SQL-friendly string form 
    columnListString = ",".join([
        f"[{c}]"
        for c in columnList
    ])
    ## Values to insert, in SQL-friendly string form
    valuesString = ",".join(
        [
                f"'{vidName}'",
                f"'{vidName}'",
                f"'{sport}'",
                f"{endpointID}",
                "1" if multipleVideoEvent else "0",
                str(samplingProportion),
                "1" if audioTranscript else "0",
                f"'{databaseID}'",
        ])
    ## Build query
    insertQuery = f"""
    INSERT INTO AzureBlobVideos ({columnListString})
    VALUES ({valuesString})
    """
    logging.info(f"AzureBlobVideos query: {insertQuery}")
    run_sql_query(insertQuery)

def wait_for_copy(blob):
    props = blob.get_blob_properties()
    while props.copy.status == 'pending':
        sleep(5)
        props = blob.get_blob_properties()
    return props