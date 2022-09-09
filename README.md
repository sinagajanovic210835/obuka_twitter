# Data Engineering Training

## • Phase I - access to data sources
      
      
## Twitter API overview

The Twitter API enables programmatic access to Twitter in unique and advanced ways. It can be used to analyze, learn from, 
and interact with Tweets, Direct Messages, users, and other key Twitter resources. Every day, developers around the world 
work with the Twitter API to create, study, and improve the public conversation happening on Twitter.
The Twitter API is a set of programmatic endpoints that can be used to understand or build the conversation on Twitter.
With Essential access, you can get access to Twitter API v2 which includes:
    • Retrieve 500,000 Tweets per month 
    • 1 Project per account 
    • 1 App environment per Project 
    • Limited access to standard v1.1 (only media endpoints) 
    • No access to premium v1.1, or enterprise 
	Tweets are the basic building block of all things Twitter. The Tweet object has a long list of ‘root-level’ fields, such 
  as id, text, and created_at. 
  Tweet objects are also the ‘parent’ object to several child objects including user, media, poll, and place. Use the field 
  parameter tweet.fields when requesting these root-level fields on the Tweet object.

Object model contents: 

    • Tweet object
    • User object
    • Space object 
    • List object
    • Media object 
    • Pool object
    • Place object 
      
	Tweet object fields are id (default), text (default), attachments, author_id, context_annotations,
  conversation_id, created_at, entities, geo, in_reply_to_user_id, lang, non_public_metrics, 
  organic_metrics, possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets, 
  reply_settings, source, withheld.
	  The User object contains Twitter user account metadata describing the referenced user. The user object 
  is the primary object returned in the users lookup endpoint. When requesting additional user fields on 
  this endpoint, simply use the fields parameter user.fields.
	User object fields are id (default), name (default), username (default), created_at, description, entities, 
  location, pinned_tweet_id, profile_image_url,   protected, public_metrics, url, verified, withheld.


  Twitter tool for generating API requests: https://developer.twitter.com/apitools/api

## • Phase II – data collection
      
    The project used data obtained from the Twitter Api, which allows access to Twitter through various endpoints. 
    For batch data, api.twitter.com/2/tweets/search/recent endpoint was used, which allows access to tweets from the
    last 7 days, according to a given filter(content of some word in the text, hashtag, etc.), as well as a combination 
    of 2 and more criteria. The results arrive in the form of a JSON response containing:
      • data, which contains a series of Tweet objects (maximum 100 per page)
      • includes, which contains additional data, which are defined in the request, e.g. data about the users who created 
        the tweet or are mentioned in it, data about the media (images, video) included in the tweet, etc. In this project, 
        User and Media data were included.
      • meta, which contains, among few other things, a next_token field containing a token for the next page of results.
      
      Apache NiFi with InvokeHttp processor was used to connect to the endpoint, data and includes were separated, and files were stored 
      in HDFS in the order they arrived. Since some of the tweets with the same ID were repeated in more then one response pages, 
      first, using Apache NiFi, a field last_modified was added to the tweets, which contained the lastmodified time of the file from 
      HDFS containing the tweet, then, using the Apache Spark program, the list of users and media is extracted from the includes field, 
      and stored in HDFS, the tweets with the same ID that had the older timestamps were discarded, a username field with a username of 
      a creator of a tweet was added into tweet, and the cleaned tweet data was repartitioned into 60-80 MB files and saved back to HDFS. 

## • Phase III - batch data processing
      
      The obtained cleaned batch data was ingested into Apache Druid, for further analysis, and also the Apache Spark application 
      was implemented, which saved the obtained aggregated data into tables in the PostgreSQL database. 
      
## • Phase IV - streaming data processing	
            
      For streaming data, a Docker container containing a python program was used, which connects to 
      api.twitter.com/2/tweets/search/stream, and writes incoming tweets to files in the folder named 
      files, from where they are downloaded by Apache NiFi with the GetFile processor, and writen to the
      Kafka topic. For streaming processing, data from Kafka was ingested into Apache Druid, and the Spark 
      Streaming application was implemented, which appends the obtained results to the tables in the PostgreSQL database.


## • Phase V - presentation of result
      
      For presentation of results, Apache Superset and Metabase BI tools were used, with connections to Apache Druid and PostgreSQL databases.

      Finally, a shell script is implemented, which boots two docker-compose files, connects PostgreSql and Druid to superset, and optionally 
      schedules a crontab in the spark-master container for the spark-streaming part of the application.
      
Technologies used:

    • Apache Kafka
    • Apache NiFi
    • Apache Spark
    • Spark Streaming
    • HDFS
    • Apache Druid
    • PostgreSQL
    • Docker, Docker-Compose
    • Apache Superset
    • Metabase
    

