import tweepy


# __ insert your own keys here __
API_KEY = "WYZhoPgdtadlYCOe0ujqPJjQ5"
API_KEY_SECRET = "p645b1JVNSkeBViZJIyQ9vxXrAYHYfDs3LQu7M1YdFSvrd3uDE"
ACCESS_TOKEN = "2805285709-wYwufhQFA0RG7Rw7kkOcrnXlm6HgoedynSX5oQc"
ACCESS_TOKEN_SECRET = "bpaX8HXbC7YQVJ4rmLCV7Rkz2yg0SK9MM37MCUiETflhj"

# __ authentication with Twitter API __
auth = tweepy.OAuth1UserHandler(API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

# __ verify credentials __
try:
    api.verify_credentials()
    print("Authentication OK")
except Exception as e:
    print(f"Error during the authentication: {e}")


# __ get tweets from your timeline __
def get_home_timeline():
    try:
        tweets = api.home_timeline(count=10)  # Recupera gli ultimi 10 tweet dalla timeline
        for tweet in tweets:
            print(f"{tweet.user.name} ha twittato: {tweet.text}")
    except Exception as e:
        print(f"Errore nel recuperare i tweet: {e}")


# Function to retrieve liked tweets
def get_liked_tweets(count=10):
    try:
        liked_tweets = api.get_favorites(count=count)  # Fetch liked tweets
        for tweet in liked_tweets:
            print(f"{tweet.user.name} tweeted: {tweet.text}")
    except Exception as e:
        print(f"Error retrieving liked tweets: {e}")


# Execute the function
get_liked_tweets(count=10)  # You can adjust 'count' to fetch more or fewer tweets


# get_home_timeline()
