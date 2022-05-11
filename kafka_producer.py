
from kafka import KafkaProducer
import json
from datetime import datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
from datetime import timezone
import twitter_credentials as tc
import tweepy

access_token = tc.access_token
access_token_secret = tc.access_token_secret
consumer_key = tc.consumer_key
consumer_secret = tc.consumer_secret


Artist_spaces=['lil nas x', 'meduza', 'lofi fruits music', 'los legendarios', 'tarcísio do acordeon', 'lunay', 'the kid laroi', 'lewis capaldi', 'rosalía', 'bryant myers', 'ava max', 'jay wheeler', 'marília mendonça', 'shawn mendes', 'kodak black', 'nio garcia', 'justin quiles', 'reik', 'tory lanez', 'sebastian yatra', '2 chainz', 'kygo', 'yoasobi', 'pop smoke', 'glass animals', 'bad bunny', 'juice wrld', 'myke towers', 'doja cat', 'giveon', 'harry styles', 'nf', '24kgoldn', 'iann dior', 'cardi b', 'christian nodal', '6lack', 'dababy', 'nicki nicole', 'lil tecca', 'miracle tones', 'pooh shiesty', 'tones and i', 'karol g', 'os barões da pisadinha', 'joji', 'lil mosey', 'trippie redd', 'rod wave', 'playboi carti', 'tate mcrae', 'imanbek', 'youngboy never broke again', 'anne-marie', 'zayn', 'a7s', 'vize', 'lil tjay', 'polo g', 'summer walker', 'calvin harris', 'gusttavo lima', 'taylor swift', 'imagine dragons', 'selena gomez', 'miley cyrus', 'ozuna', 'camilo', 'anderson .paak', 'luke combs', 'metro boomin', 'swae lee', '5 seconds of summer', 'jorge & mateus', 'billie eilish', 'roddy ricch', 'arijit singh', 'nirvana', 'metallica', 'beyoncé', '2pac', 'sean paul', 'pharrell williams', 'radiohead', 'bob marley & the wailers', 'alicia keys', 'david guetta', 'black eyed peas', 'elton john', 'the rolling stones', 'gucci mane', 'shakira', 'justin timberlake', 'rick ross', 'g-eazy', 'ninho', 'daddy yankee', 'chris brown', 'jason derulo', 'linkin park', 'ac/dc', 'tyler, the creator', 'michael jackson', 'fleetwood mac', 'onerepublic', 'pitbull', 'john legend', 'don omar', 'duki', 'ñengo flow', 'french montana', 'arctic monkeys', 'mac miller', 'the neighbourhood', 'red hot chili peppers', '50 cent', '$uicideboy$', 'britney spears', 'wisin & yandel', 'luis miguel', 'usher', 'panic! at the disco', 'benny blanco', 'ed sheeran', 'kanye west', 'xxxtentacion', 'anuel aa', 'future', 'one direction', 'die drei ???', 'lil wayne', 'sech', 'coldplay', 'daniel caesar', 'the beatles', 'machine gun kelly', 'sam smith', 'wisin', '21 savage', 'blackpink', 'katy perry', 'blackbear', 'jay-z', 'lenny tavárez', 'ellie goulding', 'nle choppa', 'justin bieber', 'the weeknd', 'bts', 'ariana grande', 'j balvin', 'dua lipa', 'eminem', 'rauw alejandro', 'bruno mars', 'khalid', 'farruko', 'maluma', 'maroon 5', 'sia', 'queen', 'kendrick lamar', 'demi lovato', 'lana del rey', 'halsey', 'marshmello', 'ty dolla $ign', 'nicky jam', 'wiz khalifa', 'sza', 'kali uchis', 'lil durk', 'olivia rodrigo', 'j. cole', 'tiësto', 'diplo', 'tyga', 'snoop dogg', 'daft punk', 'frank ocean', 'a$ap rocky', 'p!nk', 'alan walker', 'avicii', 'romeo santos', 'the chainsmokers', 'camila cabello', 'adele', 'chance the rapper', 'robin schulz', 'bebe rexha', 'darell', 'akon', 'pink floyd', 'juhn', "guns n' roses", 'dj snake', 'chencho corleone', 'anitta', 'russ', 'miguel', 'bibi blocksberg', 'led zeppelin', 'lyanno', 'moneybagg yo', 'burna boy', 'quavo', 'don toliver', 'lauv', 'alok', 'jack harlow', 'banda ms de sergio lizárraga', 'little mix', 'sch', 'topic', 'offset', 'dmx', 'fall out boy', 'jhené aiko', 'logic', 'martin garrix', 'bizarrap', 'the notorious b.i.g.', 'conan gray', 'tame impala', 'flo rida', 'melanie martinez', 'migos', 'wesley safadão', 'lil yachty', 'labrinth', 'major lazer', 'clairo', 'r3hab', 'ajr', 'becky g', 't-pain', 'ludovico einaudi', 'niall horan', 'christina aguilera', 'bruno & marrone', 'charlie puth', 'dr. dre', 'khea', 'gorillaz', 'hozier', 'dermot kennedy', 'ne-yo', 'u2', 'el alfa', 'brent faiyaz', 'david bowie', 'kevin gates', 'zara larsson', 'bring me the horizon', 'jeremy zucker', 'dj khaled', 'julia michaels', 'jul', 'yandel', 'kehlani', 'felix jaehn', 'hvme', 'h.e.r.', 'bon jovi', 'kesha', 'morat', 'abba', 'brockhampton', 'marc anthony', 'yg', 'riton', 'zion & lennox', 'alessia cara', 'creedence clearwater revival', 'childish gambino', 'clean bandit', 'maren morris', 'foo fighters', 'yungblud', 'partynextdoor', 'zedd', 'bryson tiller', 'tainy', 'raye', 'ashnikko', 'jeremih', 'the killers', 'league of legends', 'rex orange county', 'alejandro fernández', 'king von', 'johann sebastian bach', 'post malone', 'lil peep', 'masked wolf', 'green day', 'john mayer', 'twice', 'natti natasha', 'mother mother', 'c. tangana', 'pritam', 'piso 21', 'carin leon', 'chris stapleton', 'system of a down', 'madison beer', 'ski mask the slump god', 'blink-182', 'paramore', 'ynw melly', 'internet money', 'manuel turizo', 'joel corry', 'lady gaga', 'nicki minaj', 'dalex', 'arcangel', 'jhay cortez', 'bonez mc', 'lil uzi vert', 'big sean', 'kid cudi', 'meek mill', 'rihanna', 'drake', 'lil baby', 'twenty one pilots', 'travis scott', 'grupo firme', 'morgan wallen', 'a boogie wit da hoodie', 'young thug', 'saweetie', 'james arthur', 'florida georgia line', 'gunna', 'silk sonic', 'megan thee stallion', 'nav']



def cleantweet(data):
    rawtweet = json.loads(data)
    tweet = {}
    tweet["date"] = datetime.strptime(rawtweet["created_at"], '%a %b %d %H:%M:%S %z %Y')\
        .replace(tzinfo=timezone.utc).astimezone(tz=None).strftime('%Y-%m-%d %H:%M:%S')
    tweet["user"] = rawtweet["user"]["screen_name"]
    if "extended_tweet" in rawtweet:
        tweet["text"] = rawtweet["extended_tweet"]["full_text"]
    else:
        tweet["text"] = rawtweet["text"]
    return json.dumps(tweet)


class StdOutListener(StreamListener):

    def __init__(self, ttopic_name, api):
        self.topic_name = ttopic_name
        self.api = api

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""

        folder = "data"
        filename = "all_tweets"
        # newdata = cleantweet(data)
        newdata = data

        producer.send(self.topic_name, newdata)
        print(newdata)
        txt_name = folder+"/"+filename+".txt"
        with open(txt_name, 'a') as f:
            f.write(newdata)
            f.write('\n')
        return True

    def on_error(self, status):
        print("Error received in kafka producer " + repr(status))
        return True  # Don't kill the stream

    def on_timeout(self):
        return True  # Don't kill the stream


producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(
    0, 10, 1), value_serializer=lambda m: json.dumps(m).encode('ascii'))

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
# https://github.com/njanmo/kafka-producer-twitter/blob/master/twitter_producer.py


i = "tw"  # topic name

l = StdOutListener(i, api)
stream = Stream(auth, l, tweet_mode='extended')
stream.filter(track=Artist_spaces, languages=["en"])

# for i in Topic_list:
#     my_index = Topic_list.index(i)
#     artist = Artist_spaces[my_index]

#     l = StdOutListener(i, api)
#     stream = Stream(auth, l, tweet_mode='extended')
#     stream.filter(track=[artist], languages=["en"])




