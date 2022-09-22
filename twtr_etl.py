from os import access
import pandas as pd
import tweepy
import s3fs
import json
from datetime import datetime

def run_twitter_etl():

    access_key = "VVCFNaM4AvsBzzxWrz9A10AET"
    access_secret = "XH9f83zCE6cZaJg007iUxlhqHymdhywdIybMUMFp8l6icb6lIl"
    consumer_key = "935506988091314176-IO7fMAXcXEjzzGcMJztKPGitOen44XY"
    consumer_secret = "j0TES5nfnTmj4tfXUFgNsBYxvKmsqXHicJvphunNUEpJk"

    #Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key,consumer_secret)

    #Create API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name = '@elonmusk', count = 250 , include_rts = False,tweet_mode = 'extended')

    #print(tweets)

    tweet_list = []

    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {'user': tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}

        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)

    df.to_csv("s3://hitali-twitter-airflow/elonmusk_tweet.csv")