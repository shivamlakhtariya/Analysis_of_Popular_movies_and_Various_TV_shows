# -*- coding: utf-8 -*-
"""
Created on Sun Oct 27 11:04:11 2019

@author: nandh
"""
#to extract data using api
import requests
import json
#response = requests.get("https://api.themoviedb.org/3/movie/9/credits?api_key=fc9f950af50cb67033d50bd229ba4fcd")
json_data = {}
count = 0
movie_list = []
for a in range(1,51):
    response = requests.get("https://api.themoviedb.org/3/movie/popular?api_key=fc9f950af50cb67033d50bd229ba4fcd&language=en-US&page="+str(a))
    json_data[a] = response.json()
    for b in range(0,20):
        movie_list.append(json_data[a]["results"][b]["id"])
        
json_data = {}
for a in range(0,1000):
    response = requests.get("https://api.themoviedb.org/3/movie/"+str(movie_list[a])+"/reviews?api_key=fc9f950af50cb67033d50bd229ba4fcd")
    if response.status_code == 200:
       json_data[count] = response.json()
       count = count+1

 

#Writing in the data in a json format        
filename = "C:/Users/nandh/Desktop/dap.json"
output_file = open(filename, 'w')
j = json.dumps(json_data)
output_file.write(j)
output_file.close()



#AWS server
# Connecting and inserting the extracted JSON data into MongoDB Database Collection.
import pymongo
try:
    dbconn = pymongo.MongoClient("mongodb://dapgrpl:dapgrpl@18.203.68.115/")
    tmdb = dbconn["TMDB"]
    movie_all = tmdb["Movie_reviews"]
    collname = tmdb.list_collection_names()
    if "Movie_reviews" in collname:
        movie_all.drop()
    movie_all = tmdb["Movie_reviews"]
    mongo_id = []
    for a in range(1000,2000):
        mongo_ins = movie_all.insert_one(json_data[a])
        mongo_id.append(mongo_ins.inserted_id)
    print("Inserted full data of movies into MongoDB database.\n")
except (Exception, pymongo.errors.PyMongoError) as mgerr:
    print("Error while inserting data into MongoDB\n", mgerr)
finally:
    print("Proceeding to extract data from MongoDB to create a Dataframe...\n")


# Extracting data from the MongoDB 
mongo_data = []
collection = tmdb['Movie_reviews']
cursor = collection.find({})
for document in cursor:
     mongo_data.append(document)    

json_data = mongo_data

     

#Converting into structured dataframe
movie_id = []
movie_author = []
movie_content = []
movie_url = []

for i in range(0,len(movie_list)):
    if json_data[i]['total_pages'] != 0: 
        for b in range(0, json_data[i]['total_results']):
            movie_id.append(json_data[i]['id'])
            movie_author.append(json_data[i]['results'][b]['author'])
            movie_content.append(json_data[i]['results'][b]['content'])
            movie_url.append(json_data[i]['results'][b]['url'])
    
import pandas as pd    
data = {'id': movie_id, 'author': movie_author, 'content': movie_content, 'url': movie_url }
struct_data = pd.DataFrame(data)





 



# data cleaning after structuring     
struct_data['id'] = pd.to_numeric(struct_data['id'])
struct_data['author'] = struct_data['author'].apply(str)
struct_data['content'] = struct_data['content'].apply(str)
struct_data['url'] = struct_data['url'].apply(str)

for i in range(0,1687):
    struct_data['content'][i] = struct_data['content'][i].replace('\n', '')
    struct_data['content'][i] = struct_data['content'][i].replace('\r', '')
    

# inserting table into postgres
import pandas.io.sql as sqlio
sqlio.to_sql(struct_data, 'movies_reviews', 'postgresql+psycopg2://dapgrpl:dapgrpl@18.203.68.115/tmdb', if_exists = 'replace')


#importing from postgres
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
sql = "SELECT id, title, author, content FROM movies_reviews INNER JOIN movie_full ON movies_reviews.id = movie_full.mov_id;"
try:
    dbConnection = psycopg2.connect(
            user = "dapgrpl",
            password = "dapgrpl",
            host = "18.203.68.115",
            database = "tmdb")
    movie_dataframe = sqlio.read_sql_query(sql, dbConnection)
except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
finally:
    if(dbConnection): dbConnection.close()
 

sql = "Select tv_review.author, tv_review.content from movies_reviews INNER JOIN tv_review ON movies_reviews.author = tv_review.author;"
try:
    dbConnection = psycopg2.connect(
            user = "dapgrpl",
            password = "dapgrpl",
            host = "18.203.68.115",
            database = "tmdb")
    movie_dataframe1 = sqlio.read_sql_query(sql, dbConnection)
except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
finally:
    if(dbConnection): dbConnection.close()




sql = "Select prod_cntry, mov_id, title, genre_name, author, content, popularity  from movies_reviews INNER JOIN movie_full ON movies_reviews.id = movie_full.mov_id;"
try:
    dbConnection = psycopg2.connect(
            user = "dapgrpl",
            password = "dapgrpl",
            host = "18.203.68.115",
            database = "tmdb")
    movie_dataframe2 = sqlio.read_sql_query(sql, dbConnection)
except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
finally:
    if(dbConnection): dbConnection.close()


    
import nltk.sentiment.vader 
from nltk.corpus import stopwords
import nltk.tokenize as nt   
from nltk.stem import PorterStemmer 
# Tokenization
word_tokens = []    
tokenizer = nt.RegexpTokenizer(r'\w+')
for i in range(0, len(movie_dataframe)):
    word_tokens.append(tokenizer.tokenize(movie_dataframe['content'][i]))

 
#Stop word removal
stop_words = set(stopwords.words('english'))     

 
movie_dataframe['word_list'] = movie_dataframe['content'].apply(lambda x: [item for item in x.split() if item not in stop_words])
          



movie_dataframe['sentiment_scores'] = ""
movie_dataframe['total_score'] = ""
movie_dataframe['review'] = ""
from afinn import Afinn
af = Afinn()

for i in range(0, len(movie_dataframe)):
        movie_dataframe['sentiment_scores'][i] = [af.score(article) for article in movie_dataframe['word_list'][i]]





for i in range(0,len(movie_dataframe)):
    movie_dataframe['total_score'][i] = sum(movie_dataframe['sentiment_scores'][i])

sentiment_category = ['positive' if score > 0 
                          else 'negative' if score < 0 
                              else 'neutral' 
                                  for score in movie_dataframe['total_score']]


movie_dataframe['review'] = sentiment_category







#Interactive word plot
%matplotlib inline
import scattertext as st
import re, io
from pprint import pprint
import pandas as pd
import numpy as np
from scipy.stats import rankdata, hmean, norm
import spacy as sp
import os, pkgutil, json, urllib
from urllib.request import urlopen
from IPython.display import IFrame
from IPython.core.display import display, HTML
from scattertext import CorpusFromPandas, produce_scattertext_explorer
display(HTML("&lt;style>.container { width:98% !important; }&lt;/style>"))



#Corpus matrix
nlp = st.WhitespaceNLP.whitespace_nlp
corpus = st.CorpusFromPandas(movie_dataframe, 
                              category_col='review', 
                              text_col='content',
                              nlp=nlp).build()

#top 10 frequent words
print(list(corpus.get_scaled_f_scores_vs_background().index[:10]))
term_freq_df = corpus.get_term_freq_df()

#Top 10 frequent positive words
term_freq_df['positive Score'] = corpus.get_scaled_f_scores('positive')
pprint(list(term_freq_df.sort_values(by='positive Score', ascending=False).index[:10]))


#Top 10 frequent negative words
term_freq_df['negative score'] = corpus.get_scaled_f_scores('negative')
pprint(list(term_freq_df.sort_values(by='negative score', ascending=False).index[:10]))

#Plot 1
html = st.produce_scattertext_explorer(corpus,
          category='positive',
          category_name='Positive',
          not_category_name='Negative',
          width_in_pixels=1000,
          metadata=movie_dataframe['author'])
open("C:/Users/nandh/Desktop/Convention-Visualization5.html", 'wb').write(html.encode('utf-8'))



#Plot 2
import pandas as pd
from sklearn.feature_extraction.text import TfidfTransformer
import scattertext as st
from scipy.sparse.linalg import svds


movie_dataframe['parse'] = movie_dataframe['content'].apply(st.whitespace_nlp_with_sentences)

#corpus matrix
corpus = (st.CorpusFromParsedDocuments(movie_dataframe,
                                       category_col='review',
                                       parsed_col='parse')
              .build()
              .get_stoplisted_unigram_corpus()) 

corpus = corpus.add_doc_names_as_metadata(corpus.get_df()['author'])

# Calculating the Eigen matrix of the corpus.....
embeddings = TfidfTransformer().fit_transform(corpus.get_term_doc_mat())
u, s, vt = svds(embeddings, k=1000, maxiter=20000, which='LM')
projection = pd.DataFrame({'term': corpus.get_metadata(), 'x': u.T[0], 'y': u.T[1]}).set_index('term')


#Plot 2
category = 'positive'
scores = (corpus.get_category_ids() == corpus.get_categories().index(category)).astype(int)
html = st.produce_pca_explorer(corpus,
                               category=category,
                               category_name='positive',
                               not_category_name='negative',
                               metadata=movie_dataframe['author'],
                               width_in_pixels=1000,
                               show_axes=False,
                               use_non_text_features=True,
                               use_full_doc=True,
                               projection=projection,
                               scores=scores,
                               show_top_terms=False)
open("C:/Users/nandh/Desktop/Convention-Visualization_1.html", 'wb').write(html.encode('utf-8'))


import plotly.graph_objects as go
from plotly.offline import plot

#heatmap
fig1 = go.Figure(data= go.Heatmap(
        x=movie_dataframe1['author'],
        y=movie_dataframe['review'],
        z=movie_dataframe['total_score'],
        colorscale= 'Viridis'))

 

fig1.update_layout(
        title='Total Score Vs Review',
        xaxis_nticks=36)
plot(fig1)


# Author vs popularity 
fig2 = go.Figure(data=[go.Bar(
        x=movie_dataframe1['author'][:80],
        y=movie_dataframe2['popularity'],
        )])

fig2.update_layout(
        title='Author Vs Popularity',
        xaxis_nticks=36, plot_bgcolor='white')

plot(fig2)
 

# genre vs total_score
fig6 = go.Figure()
fig6.add_trace(go.Bar(
        x = movie_dataframe2['genre_name'],
        y = movie_dataframe['total_score'],
        #name= 'positive',
        marker_color = 'darkviolet',
        width = [0.8]))

fig6.update_layout(
        title='Genre Vs Sentiment_score',
        xaxis_nticks=36, plot_bgcolor='white')

plot(fig6)


