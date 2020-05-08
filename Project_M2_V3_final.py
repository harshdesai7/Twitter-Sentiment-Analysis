# Databricks notebook source
# MAGIC %md #Sentiment Analysis

# COMMAND ----------

# MAGIC %md ##Objective: To Design a model to extract, identify and characterize sentitment of the Tweet

# COMMAND ----------

# MAGIC %md ###Visualizing Global Impact of CoronaVirus

# COMMAND ----------

# MAGIC %md ####Extracting country and US state data

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/harshdesai7/Titanic/master/Country.csv
# MAGIC wget https://raw.githubusercontent.com/harshdesai7/Titanic/master/state.csv

# COMMAND ----------

# MAGIC %md ####Getting Live Data from API and binding it to Spark DataFrame

# COMMAND ----------

#consuming the data provided by api.covid19api.com in json and converting it into Spark DataFrame
import json
import requests
import pandas as pd
from pandas.io.json import json_normalize
request = requests.get("https://api.covid19api.com/summary")
#request = requests.get("https://api.thevirustracker.com/free-api?global=stats")
response = json.loads(request.text)
dfOverallStats=spark.createDataFrame(json_normalize(response['Global']))
display(dfOverallStats)

# COMMAND ----------

#consuming the data provided by api.covid19api.com in json and converting it into Spark DataFrame
request = requests.get("https://api.covid19api.com/summary")
response = json.loads(request.text)
dfCountryStats=spark.createDataFrame(json_normalize(response['Countries']))
dfCountryStats.count()
#display(dfCountryStats)

# COMMAND ----------

#displaying country wise total confirmed cases using select and where clause to have latest data
from pyspark.sql.functions import col
dfToday=dfCountryStats.select('Country','TotalConfirmed').where(col('Date').like("%2020%"))
display(dfToday)

# COMMAND ----------

# MAGIC %md ####Total cases over a period of time globally

# COMMAND ----------

#Extracting information from API and displaying Bar Chart Visualization of total cases against count dateswise 
response = requests.get("https://thevirustracker.com/timeline/map-data.json")
todos = json.loads(response.text)
dfCountryData = spark.createDataFrame(json_normalize(todos['data']))
display(dfCountryData)
dfCountryData = dfCountryData.withColumn('cases',  dfCountryData['cases'].cast('int'))
display(dfCountryData.select("date","cases"))

# COMMAND ----------

# MAGIC %md ####Recovered vs Total Cases

# COMMAND ----------

#displaying Area Chart Visualization of total confimed cases cases and recovered cases against count dateswise
dfCountryData = dfCountryData.withColumn('recovered',  dfCountryData['recovered'].cast('int'))
display(dfCountryData.select("date","cases","recovered"))

# COMMAND ----------

#Reading the downloaded csv file for Mapping State and Country with the API Data
dfCountries = spark.read.csv('file:/databricks/driver/Country.csv',header=True)
#display(dfCountries)
dfstates = spark.read.csv('file:/databricks/driver/state.csv',header=True)
#display(dfstates)

# COMMAND ----------

# MAGIC %md ####World Map showing affected countries
# MAGIC ######To see individual country data, hover over the country

# COMMAND ----------

#Displaying Visualization of World wide total count of corona virus cases. Hover over a country to know it's total count
worldRDD=dfCountries.join(dfToday, dfCountries["Country"] == dfToday['Country']).select('Code','TotalConfirmed')
worldRDD=worldRDD.withColumnRenamed("Code", "country")
worldRDD=worldRDD.withColumnRenamed("TotalConfirmed", "value")
display(worldRDD)

# COMMAND ----------

#Extracting US state wise count from API in json and converting it into DataFrame 
from pyspark.sql.functions import col, expr
request = requests.get("https://api.covid19api.com/live/country/us/status/confirmed")
response = json.loads(request.text)
data=spark.createDataFrame(response)
df1=data.orderBy("Date", ascending=False)
#display(df1)


# COMMAND ----------

display(df1)

# COMMAND ----------

#sorting the data in datewise descending to filter latest count per state
currdate=(df1.select('*').orderBy('Date','Province',ascending=False).limit(56))
display(currdate)

# COMMAND ----------

# MAGIC %md ####US Map showing affected states
# MAGIC ######To see individual states data, hover over the states

# COMMAND ----------

#Displaying Visualization of US state wide total count of corona virus cases. Hover over a state to know it's total count
stateRDD=dfstates.join(currdate, dfstates["State"] == currdate['Province']).select('Code','Confirmed')
stateRDD=stateRDD.withColumnRenamed("Code", "state")
stateRDD=stateRDD.withColumnRenamed("Confirmed", "value")
display(stateRDD)

# COMMAND ----------

# MAGIC %md  ###Tableau Visualizations

# COMMAND ----------

# MAGIC %md ####Please <a href="https://public.tableau.com/views/State_15881828842450/Dashboard2?:display_count=y&publish=yes&:origin=viz_share_link" target=_blank> click here </a> to view the visualizations performed in Tableau

# COMMAND ----------

# MAGIC %md ####Streaming Tweets using Tweepy API

# COMMAND ----------

#installing tweepy in order to use Tweepy API

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install tweepy

# COMMAND ----------

# MAGIC %md ####Authenticating with Twitter Credentials using OAuth

# COMMAND ----------

#After registering on Developer Twitter website and retreiving the Keys
#We are validating it using OAuth Authentication to gain access of the tweets provided by the tweepy API
import tweepy
import csv
import ssl
import time
import socket
from tweepy.auth import OAuthHandler
from tweepy import Stream
from requests.exceptions import Timeout, ConnectionError
from requests.packages.urllib3.exceptions import ReadTimeoutError
consumer_key = 'EkJJSAt55n5v1vcUo0jFg'
consumer_secret = 'o8sE5KA9NH5blHjIPBnBOiUz48D9buo3UmBMD0Js'
access_token = '59322976-kRknlhcWTcnfs6dk4aKXRlhnWD0P7K3mD2h44xt7d'
access_token_secret = 'u7tQzPZHbMrOjkAZYrNPx1FElqJejSqFWcCEwygr9dPvM'
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# COMMAND ----------

# MAGIC %md ####Filtering Tweets relating to Coronavirus

# COMMAND ----------

#Mentioning the Keywords in order to filter out Keys related to CoronaVirus
import csv
import ssl
import time
from requests.exceptions import Timeout, ConnectionError
from requests.packages.urllib3.exceptions import ReadTimeoutError
# Setting up the keywords, hashtag or mentions we want to listen
keywords = ["#Covid19", "Covid19","Coronavirus"]

# Set the name for CSV file where the tweets will be saved
filename = "myfile"

# COMMAND ----------

# MAGIC %md ####Extracting Filtered Tweets

# COMMAND ----------

# MAGIC %md #####Starts extracting Tweet, Run for 10 minutes to extract approximately 2000 Tweets. Have to manually stop execution to stop the streaming

# COMMAND ----------


class StreamListener(tweepy.StreamListener):

    def on_status(self, status):

        try:
            # saves the tweet object
            tweet_object = status

            # Checks if its a extended tweet (>140 characters)
            if 'extended_tweet' in tweet_object._json:
                tweet = tweet_object.extended_tweet['full_text']
            else:
                tweet = tweet_object.text

#Convert all named and numeric character references (e.g. &gt;, &#62;, &#x3e;) in the strings to their corresponding Unicode characters
            tweet = (tweet.replace('&amp;', '&').replace('&lt;', '<')
                     .replace('&gt;', '>').replace('&quot;', '"')
                     .replace('&#39;', "'").replace(';', " ")
                     .replace(r'\u', " "))

#Save the keyword that matches the stream
            keyword_matches = []
            for word in keywords:
                if word.lower() in tweet.lower():
                    keyword_matches.extend([word])

            keywords_strings = ", ".join(str(x) for x in keyword_matches)

#Save other information from the tweet
            user = status.author.screen_name
            timeTweet = status.created_at
            source = status.source
            tweetId = status.id
            tweetUrl = "https://twitter.com/statuses/" + str(tweetId)

#Exclude retweets, too many mentions and too many hashtags
            if not any((('RT @' in tweet, 'RT' in tweet,
                       tweet.count('@') >= 2, tweet.count('#') >= 3))):

#Saves the tweet information in a new row of the CSV file
                writer.writerow([tweet, keywords_strings, timeTweet,
                                user, source, tweetId, tweetUrl])

        except Exception as e:
            print('Encountered Exception:', e)
            pass


def work():

#Opening a CSV file to save the gathered tweets
    with open(filename+".csv", 'w') as file:
        global writer
        writer = csv.writer(file)

#Add a header row to the CSV
        writer.writerow(["Tweet", "Matched Keywords", "Date", "User",
                        "Source", "Tweet ID", "Tweet URL"])

#Initializing the twitter Stream and also trying to convert other languages to english
        try:
            streamingAPI = tweepy.streaming.Stream(auth, StreamListener())
            streamingAPI.filter(languages=["en"],track=keywords)

#Stop temporarily when hitting Twitter rate Limit
        except tweepy.RateLimitError:
            print("RateLimitError...waiting ~15 minutes to continue")
            time.sleep(1001)
            streamingAPI = tweepy.streaming.Stream(auth, StreamListener())
            streamingAPI.filter(languages=["en"],track=keywords)

#Stop temporarily when getting a timeout or connection error
        except (Timeout, ssl.SSLError, ReadTimeoutError,
                ConnectionError) as exc:
            print("Timeout/connection error...waiting ~15 minutes to continue")
            time.sleep(1001)
            streamingAPI = tweepy.streaming.Stream(auth, StreamListener())
            streamingAPI.filter(languages=["en"],track=keywords)

#Stop temporarily when getting other errors
        except tweepy.TweepError as e:
            if 'Failed to send request:' in e.reason:
                print("Time out error caught.")
                time.sleep(1001)
                streamingAPI = tweepy.streaming.Stream(auth, StreamListener())
                streamingAPI.filter(languages=["en"],track=keywords)
            else:
                print("Other error with this user...passing")
                pass


if __name__ == '__main__':

    work()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

#reading the streamed tweets from csv file
from pyspark.sql.functions import col
dfTweets = spark.read.csv('file:///databricks/driver/myfile.csv',inferSchema='true',header='true')
dfTweets.count()


# COMMAND ----------

# MAGIC %md ###Data Cleaning

# COMMAND ----------

display(dfTweets)

# COMMAND ----------

# MAGIC %md ####Converting DataFrame to List

# COMMAND ----------

#taking tweet column as list
from pyspark.sql.functions import col
tweetslist = dfTweets.select(col("Tweet")).collect()
tweetslist

# COMMAND ----------

# MAGIC %md ####Cleaning the Tweets Using Regular Expression

# COMMAND ----------

#UDF for Removing Patterns
def remove_pattern(input_txt, pattern):
    r = re.findall(pattern, input_txt)
    for i in r:
        input_txt = re.sub(i, '', input_txt)        
    return input_txt

# COMMAND ----------

import numpy as np
import re
rem_link = []
list1=[]
for i in range(0,len(tweetslist)-1):
  # Using Regex to remove row from tweets
  list1.append(remove_pattern(str(tweetslist[i]), "Row[A-Za-z0-9./]*"))
list2 = []
for i in range(0,len(tweetslist)-1):
   # Using Regex to remove the word tweet from tweets
  list2.append(remove_pattern(str(list1[i]), "Tweet[A-Za-z0-9./]*"))
list2
for i in range(0,len(list2)-1):
   # Removing url's from the tweets list
  rem_link.append(remove_pattern(str(list2[i]), "https?://[A-Za-z0-9./]*"))
rem_retweet = []
for i in range(0,len(rem_link)-1):
   # Using Regex to remove retweets
  if not rem_link[i].startswith('RT'):
    rem_retweet.append(rem_link[i])
rem_specialChar = []
for i in range(0,len(rem_retweet)-1):
  #elem = re.sub( '[^a-z0-9A-Z]', ' ', cleaned_list1[i])
  rem_specialChar.append(re.sub( '[^a-z0-9A-Z]', ' ', rem_retweet[i]))
clean = []
for i in range(0,len(rem_specialChar)-1):
  clean.append(re.sub( 's/^\s+|\s+$|\s+(?=\s)//g', ' ', rem_specialChar[i]))
cleaned_tweets = clean
for x in range(len(cleaned_tweets)):
  #stripping the whitespace
  cleaned_tweets[x]=cleaned_tweets[x].strip()
cleaned_tweets

# COMMAND ----------

# MAGIC %md ###Sentiment Analysis
# MAGIC 
# MAGIC #####We wanted to train the model on a constant data. As the streaming data keeps changing, we will be doing our predictions using stream data which was exported into a csv
# MAGIC #####The below code would predict a label as an output. This data set has two different polarities i.e. negative and positive
# MAGIC #####We would be predicting the outcome in the label column
# MAGIC #####Sampled Labeled Data is taken from Sentiment-140 Dataset

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://raw.githubusercontent.com/harshdesai7/Titanic/master/training.csv
# MAGIC #wget http://cs.stanford.edu/people/alecmgo/trainingandtestdata.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

#Installing findspark library

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install findspark

# COMMAND ----------

import findspark
import re
findspark.init()
import pyspark as ps
import warnings
from pyspark.sql import SQLContext

# COMMAND ----------

# MAGIC %md ####Reading the Tweets file in Databricks

# COMMAND ----------

df = spark.read.csv('file:///databricks/driver/training.csv',header='false', inferSchema ='true')
#df = spark.read.csv('file:///databricks/driver/training.1600000.processed.noemoticon.csv',header='false', inferSchema ='true')

# COMMAND ----------

# MAGIC %md ####Renaming Columns and Selecting only necessary Columns

# COMMAND ----------

df = df.withColumnRenamed("_c5","text")
df = df.withColumnRenamed("_c0","target")
df=df.select("text","target")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ####Removing Null Values

# COMMAND ----------

df.count()

# COMMAND ----------

df = df.dropna()
df.count()

# COMMAND ----------

# MAGIC %md ####Splitting data into Train and Test

# COMMAND ----------

(train_set, val_set, test_set) = df.randomSplit([0.7, 0.01, 0.29], seed = 2000)

# COMMAND ----------

# MAGIC %md ###Fitting into a Pipeline

# COMMAND ----------

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier,NaiveBayes
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.ml.evaluation import RegressionEvaluator,BinaryClassificationEvaluator


tokenizer = Tokenizer(inputCol="text", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="tf")
idf = IDF(inputCol='tf', outputCol="features", minDocFreq=5)
label_stringIdx = StringIndexer(inputCol = "target", outputCol = "label")

pipeline = Pipeline(stages=[])  # Must initialize with empty list!

basePipeline=[tokenizer, hashingTF,idf,label_stringIdx]
#basePipeline=[tokenizer, idf,label_stringIdx]

# Models to compare
#Logistic regression model
lr = LogisticRegression(maxIter=10)

pl_lr = basePipeline + [lr]
pg_lr = ParamGridBuilder()\
          .baseOn({pipeline.stages: pl_lr})\
          .addGrid(lr.regParam,[0.01, .04])\
          .addGrid(lr.elasticNetParam,[0.1, 0.4])\
          .build()

#Random forest Classifier
rf = RandomForestClassifier(numTrees=50)
pl_rf = basePipeline + [rf]
pg_rf = ParamGridBuilder()\
      .baseOn({pipeline.stages: pl_rf})\
      .build() 

#Naive Bayes Classifier
nb = NaiveBayes()
pl_nb = basePipeline + [nb]
pg_nb = ParamGridBuilder()\
      .baseOn({pipeline.stages: pl_nb})\
      .addGrid(nb.smoothing,[0.4,1.0])\
      .build()

#Combining all ParamGrids
paramGrid = pg_lr + pg_rf + pg_nb

# COMMAND ----------

# MAGIC %md ###Cross validator for Pipeline
# MAGIC #####Takes 20 minutes to run

# COMMAND ----------

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(),
                          numFolds=3)  # use 3+ folds in practice
cvModel = crossval.fit(train_set)

# COMMAND ----------

# MAGIC %md ###Best Model

# COMMAND ----------

import numpy as np
cvModel.getEstimatorParamMaps()[ np.argmax(cvModel.avgMetrics)]

# COMMAND ----------

# MAGIC %md ###Worst Model

# COMMAND ----------

cvModel.getEstimatorParamMaps()[ np.argmin(cvModel.avgMetrics)]

# COMMAND ----------

# MAGIC %md ###Evaluating the Model

# COMMAND ----------

from pyspark.ml.evaluation import BinaryClassificationEvaluator
evaluator=BinaryClassificationEvaluator()
predictions = cvModel.transform(test_set)
print("Evaluating the Model, Value found to be: ",format(evaluator.evaluate(predictions),'.2f'))

# COMMAND ----------

# MAGIC %md ###Printing metrics

# COMMAND ----------

from pyspark.mllib.evaluation import BinaryClassificationMetrics

predictionAndLabels = predictions.select('prediction','label').rdd
metrics = BinaryClassificationMetrics(predictionAndLabels)
print("Precision Recall:", format(metrics.areaUnderPR,".2f"))
print("ROC:", format(metrics.areaUnderROC,".2f"))


# COMMAND ----------

# MAGIC %md ####Building the Metrics

# COMMAND ----------

import re
def paramGrid_model_name(model):
  params = [v for v in model.values() if type(v) is not list]
  name = [v[-1] for v in model.values() if type(v) is list][0]
  name = re.match(r'([a-zA-Z]*)', str(name)).groups()[0]
  return "{}{}".format(name,params)

# Resulting metric and model description
# get the measure from the CrossValidator, cvModel.avgMetrics
# get the model name & params from the paramGrid
kmeans_measures = zip(cvModel.avgMetrics, [paramGrid_model_name(m) for m in paramGrid])
metrics,model_names = zip(*kmeans_measures)

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install --upgrade pip
# MAGIC pip install numpy

# COMMAND ----------

# MAGIC %md ####Plotting the Models

# COMMAND ----------

import matplotlib.pyplot as plt

plt.clf() # clear figure
fig = plt.figure( figsize=(8, 8))
plt.style.use('fivethirtyeight')
axis = fig.add_axes([0.1, 0.3, 0.8, 0.6])
# plot the metrics as Y
#plt.plot(range(len(model_names)),metrics)
plt.bar(range(len(model_names)),metrics)
# plot the model name & param as X labels
plt.xticks(range(len(model_names)), model_names, rotation=70, fontsize=14)
plt.yticks(fontsize=14)
#plt.xlabel('model',fontsize=8)
plt.ylabel('ROC AUC (better is greater)',fontsize=14)
plt.title('Model evaluations')
display(plt.show())

# COMMAND ----------

# MAGIC %md ###Predictions
# MAGIC #### This is fairly good result however, there is further scope for improvement
# MAGIC #####0 Signifies Negative Sentiment and 1 Signifies Positive Sentiment

# COMMAND ----------

correct = predictions.where("(label = prediction)").count()
incorrect = predictions.where("(label != prediction)").count()
resultDF = sqlContext.createDataFrame([['correct', correct], ['incorrect', incorrect]], ['metric', 'value'])
display(resultDF)

# COMMAND ----------

# MAGIC %md ####Actual vs Predicted

# COMMAND ----------

counts = [predictions.where('label=1').count(), predictions.where('prediction=1').count(),
          predictions.where('label=0').count(), predictions.where('prediction=0').count()]
names = ['actual 1', 'predicted 1', 'actual 0', 'predicted 0']
display(sqlContext.createDataFrame(zip(names,counts),['Measure','Value']))

# COMMAND ----------

# MAGIC %md ###Word Cloud that describes sentiment

# COMMAND ----------

import numpy as np # linear algebra
import pandas as pd 
import matplotlib as mpl
import matplotlib.pyplot as plt
%matplotlib inline

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install wordcloud

# COMMAND ----------

# MAGIC %md #####Building the Text for WordCLoud

# COMMAND ----------


from pyspark.sql.functions import col
text = "" 

# iterating through list of rows 
for row in cleaned_tweets :
  for word in row :
    text = text + word 
  #print(word)
print(text)

# COMMAND ----------

# importing the necessery modules 
#%matplotlib inline
from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt 
import csv  
 
wordcloud = WordCloud(width=4000, height=2400).generate(text) 
wordcloud = WordCloud(
        background_color='white',
        stopwords=STOPWORDS,
        width=1000,
        height=600,
        random_state=21,
        colormap='jet',
        max_words=150,
        max_font_size=200).generate(text) 

# plot the WordCloud image 
plt.figure( figsize=(18, 12))
#plt.figure() 
plt.imshow(wordcloud, interpolation="bilinear") 
plt.axis("off") 
plt.margins(x=0, y=0) 
plt.show() 

# COMMAND ----------

from IPython.display import Image
from IPython.core.display import HTML
Image(url= "https://www.gannett-cdn.com/presto/2020/03/15/USAT/e6e30693-9224-4c89-b003-6cc4b4348528-insta-noemoji-heart.png?width=660&height=660&fit=crop&format=pjpg&auto=webp")

# COMMAND ----------

from IPython.display import Image
from IPython.core.display import HTML
Image(url= "https://upload.wikimedia.org/wikipedia/commons/e/ea/Thats_all_folks.svg")
