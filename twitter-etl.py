import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

# function

def run_twitter_etl():
    access_key = "Ir5y3mIuBPfkwGTNMIJUhwduY"
    access_secret = "ZskJz3hG788GBAIArtFlKOpJPpBZjLOSR8N9sy4lZFzO6a7GDb"
    consumer_key = "1658466268217999360-nawiIVJxuxXSGuUEtPApikK4o8W2in"
    consumer_secret = "RShVGAiqOQtLWa5IzlTknL32JuCvgxVcH0nqGaYyuDHDe"

    # twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # creating an API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@narendramodi',
                                # 200 is the maximum allowed count
                                count=200,
                                include_rts = False,
                                # Neccessary to keep full_text
                                # otherwise only the first 140 words are shown
                                tweet_mode = 'extended'
                                )

    # print(tweets)

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at
                        }
        
        tweet_list.append(refined_tweet)

        df = pd.DataFrame(tweet_list)
        df.to_csv("s3://shilpi-airflow-twitter-bucket/pm_twitter_data.csv")




        