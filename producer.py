from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
import praw
import logging

LIMIT = 700
KAFKA_TOPIC = 'redditStreams'

def receipt(logger: logging.Logger, i: int , submission: praw.models.reddit.submission.Submission) -> None:
    message = f"Produced message on topic {KAFKA_TOPIC} - {i} - with value of {submission.id}"
    logger.info(message)
    print(message)
        
def get_data() -> praw.models.listing.generator.ListingGenerator:  
    try:
        print("Scraping Subreddit Submissions")
        # Fetch submissions from both "travel" and "wanderlust" subreddits
        travel_submissions = praw.Reddit("bot1").subreddit('travel').hot(limit=LIMIT)
        wanderlust_submissions = praw.Reddit("bot1").subreddit('wanderlust').hot(limit=LIMIT)
        TravelHacks_submissions = praw.Reddit("bot1").subreddit('TravelHacks').hot(limit=LIMIT)
        solotravel_submissions = praw.Reddit("bot1").subreddit('solotravel').hot(limit=LIMIT)
        # Concatenate the submissions from both subreddits
        submissions = list(travel_submissions) + list(wanderlust_submissions) + list(TravelHacks_submissions) + list(solotravel_submissions)
    except Exception as e:
        # in case of bad request 
        print(f"Error: {e}")
        exit(1)
    return submissions        

def send_message(submission: praw.models.reddit.submission.Submission, producer: KafkaProducer) -> None:           
    producer.send(KAFKA_TOPIC, value = {
        "id": submission.id,  
        "num_comments":  vars(submission)["num_comments"],
        "score": vars(submission)["score"],
        "name": vars(submission)["name"],
        "created_utc": vars(submission)["created_utc"],
        "edited": vars(submission)["edited"],
        "spoiler": vars(submission)["spoiler"],
        "url": vars(submission)["url"],
        "title": vars(submission)["title"]
    }) 

def main() -> None:
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='producer.log',
                        filemode='w')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # scrape subreddits
    submissions = get_data()
    
    print('Kafka Producer has been initiated...')
    producer = KafkaProducer(bootstrap_servers = ['localhost:9092'], value_serializer = lambda x : dumps(x).encode('utf-8'))
    
    for i, submission in enumerate(submissions):
        receipt(logger, i, submission)
        send_message(submission, producer)

if __name__ == '__main__':
    main()