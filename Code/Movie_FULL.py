# Importing required packages
import requests
import pymongo as pm
import pandas as pd
import psycopg2 as pc2
import pandas.io.sql as sqlio
import numpy as np
import plotly.graph_objects as go
from plotly.offline import plot
import plotly.io as pio
pio.renderers.default = "svg"

# Extracting the first 1000 popular movie IDs from TMDB API.
json_data = {}
movie_list = []
try:
    for a in range(1, 51):
        response = requests.get(
            "https://api.themoviedb.org/3/movie/popular?api_key=b7e7a7c29b9bf3b91508aa9599458b5a&language=en-US&page=" + str(
                a))
        if response.status_code == 200:
            json_data[a] = response.json()
        for b in range(0, 20):
            movie_list.append(json_data[a]["results"][b]["id"])
    print("Extracted popular movie IDs from the TMDB API.\n")
except (Exception, requests.RequestException) as connerr:
    print("Error while extracting data from the TMDB API\n", connerr)
finally:
    print("Proceeding to extract the full data of the movies...\n")

# Extracting all details for the first 1000 popular movies on TMDB from API.
json_data = {}
try:
    for a in range(0, len(movie_list)):
        response = requests.get(
            "https://api.themoviedb.org/3/movie/" + str(movie_list[a]) + "?api_key=b7e7a7c29b9bf3b91508aa9599458b5a")
        if response.status_code == 200:
            json_data[a] = response.json()
    print("Extracted the full data of the movies from the TMDB API.\n")
except (Exception, requests.RequestException) as connerr:
    print("Error while extracting data from the TMDB API\n", connerr)
finally:
    print("Proceeding to input the full data of movies into MongoDB...\n")

# Connecting and inserting the extracted JSON data into MongoDB Database Collection.
try:
    dbconn = pm.MongoClient("mongodb://dapgrpl:dapgrpl@18.203.68.115/")
    tmdb = dbconn["TMDB"]
    movie_all = tmdb["MOVIE_FULL"]
    collname = tmdb.list_collection_names()
    if "MOVIE_FULL" in collname:
        movie_all.drop()
    movie_all_mng = tmdb["MOVIE_FULL"]
    mongo_id = []
    for a in range(0, len(movie_list)):
        mongo_ins = movie_all.insert_one(json_data[a])
        mongo_id.append(mongo_ins.inserted_id)
    print("Inserted full data of movies into MongoDB database.\n")
except (Exception, pm.errors.PyMongoError) as mgerr:
    print("Error while inserting data into MongoDB\n", mgerr)
finally:
    print("Proceeding to extract data from MongoDB to create a Dataframe...\n")

# Extracting the required data from MongoDB Collection to create a dataframe.
try:
    mov_list = movie_all_mng.find({}, {"_id": 0, "id": 1, "title": 1, "budget": 1, "popularity": 1, "release_date": 1,
                                   "revenue": 1, "runtime": 1, "vote_average": 1, "vote_count": 1, "adult": 1,
                                   "genres": 1, "production_companies": 1, "production_countries": 1,
                                   "spoken_languages": 1, "status": 1})
    mov_id = []
    title = []
    budget = []
    popularity = []
    release_date = []
    revenue = []
    runtime = []
    vote_average = []
    vote_count = []
    adult = []
    genre_name = []
    prod_cmpny = []
    prod_cntry = []
    languages = []
    status = []
    for a in range(0, len(movie_list)):
        mov_id.append(mov_list[a]["id"])
        title.append(mov_list[a]["title"])
        budget.append(mov_list[a]["budget"])
        popularity.append(mov_list[a]["popularity"])
        release_date.append(mov_list[a]["release_date"])
        revenue.append(mov_list[a]["revenue"])
        runtime.append(mov_list[a]["runtime"])
        vote_average.append(mov_list[a]["vote_average"])
        vote_count.append(mov_list[a]["vote_count"])
        adult.append(mov_list[a]["adult"])
        genre = []
        for b in range(0, len(mov_list[a]["genres"])):
            genre.append(mov_list[a]["genres"][b]["name"])
        genre = ','.join(map(str, genre))
        genre_name.append(genre)
        cmp = []
        for b in range(0, len(mov_list[a]["production_companies"])):
            cmp.append(mov_list[a]["production_companies"][b]["name"])
        cmp = ','.join(map(str, cmp))
        prod_cmpny.append(cmp)
        cntr = []
        for b in range(0, len(mov_list[a]["production_countries"])):
            cntr.append(mov_list[a]["production_countries"][b]["name"])
        cntr = ','.join(map(str, cntr))
        prod_cntry.append(cntr)
        lang = []
        for b in range(0, len(mov_list[a]["spoken_languages"])):
            lang.append(mov_list[a]["spoken_languages"][b]["iso_639_1"])
        lang = ','.join(map(str, lang))
        languages.append(lang)
        status.append(mov_list[a]["status"])
    print("Extracted all data from MongoDB and added them to separate lists.\n")
except(Exception, pm.errors.PyMongoError) as mgerr:
    print("Error while extracting data from MongoDB\n", mgerr)
finally:
    print("Proceeding to create a Dataframe...\n")

# Creating a Dataframe.
movie_all = pd.DataFrame(
    {"mov_id": mov_id,
     "title": title,
     "budget": budget,
     "release_date": release_date,
     "status": status,
     "revenue": revenue,
     "popularity": popularity,
     "runtime": runtime,
     "vote_average": vote_average,
     "vote_count": vote_count,
     "adult": adult,
     "genre_name": genre_name,
     "prod_cmpny": prod_cmpny,
     "prod_cntry": prod_cntry,
     "languages": languages
     })

print("Dataframe created.\n")
print("Proceeding to create database on postgresql...\n")

# Check for NA and Blank values in dataaframe and replacing them with None
movie_all = movie_all.replace('', np.nan)
movie_all = movie_all.astype(object).where(pd.notnull(movie_all), None)

# Connecting to Postgres and creating a database.
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="postgres")
    dbconn.set_isolation_level(0)
    dbcur = dbconn.cursor()
    sql = "SELECT 1 AS result FROM pg_database WHERE datname = 'tmdb';"
    dbexists = sqlio.read_sql_query(sql, dbconn)
    print("Connected to postgresql.\n")
    if dbexists.empty:
        dbcur.execute("CREATE DATABASE TMDB;")
        print("Created Database on postgresql.\n")
    else:
        print("Database TMDB already exists.\n")
    dbcur.close()
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to create Table in postgres database...\n")

# Connecting to Postgres, creating a table and inserting records from dataframe in the TMDB database.
try:
    sqlio.to_sql(movie_all, 'movie_full', 'postgresql+psycopg2://dapgrpl:dapgrpl@18.203.68.115/tmdb',
                 if_exists='replace', index=False)
    print("Created table in postgresql and inserted the data from the dataframe.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to extract data from postgres database table...\n")

# Preliminary extraction of data from postgresql
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    movie_all = sqlio.read_sql_query("SELECT * FROM movie_full;", dbconn)
    dbcur.close()
    print("Extracted all data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform transformations on the data...\n")

# Getting unique genre names, production companies, production countries, languages from the extracted data
unique_genres = list(movie_all["genre_name"])
unique_genre_names = []
for i in range(0, len(unique_genres)):
    unique_genre_names.append(unique_genres[i])
unique_genre_names = ','.join(map(str, unique_genre_names))
unique_genre_names = unique_genre_names.split(",")
unique_genre_names = list(set(unique_genre_names))

unique_cmpny = list(movie_all["prod_cmpny"])
unique_prod_cmpny = []
for i in range(0, len(unique_cmpny)):
    unique_prod_cmpny.append(unique_cmpny[i])
unique_prod_cmpny = ','.join(map(str, unique_prod_cmpny))
unique_prod_cmpny = unique_prod_cmpny.split(",")
unique_prod_cmpny = list(set(unique_prod_cmpny))

unique_cntry = list(movie_all["prod_cntry"])
unique_prod_cntry = []
for i in range(0, len(unique_cntry)):
    unique_prod_cntry.append(unique_cntry[i])
unique_prod_cntry = ','.join(map(str, unique_prod_cntry))
unique_prod_cntry = unique_prod_cntry.split(",")
unique_prod_cntry = list(set(unique_prod_cntry))

unique_lang = list(movie_all["languages"])
unique_languages = []
for i in range(0, len(unique_lang)):
    unique_languages.append(unique_lang[i])
unique_languages = ','.join(map(str, unique_languages))
unique_languages = unique_languages.split(",")
unique_languages = list(set(unique_languages))

print("Extracted unique genre names, production companies and production countries in separate lists.\n")

# Beginning visualizations on the data stored in postgres
# Extracting and Visualizing movie count as per release year
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    movie_count_year = sqlio.read_sql_query("SELECT count(mov_id) AS MOVIE_COUNT, date_part('year', cast(release_date AS DATE)) as RELEASE_YEAR from movie_full where date_part('year', cast(release_date AS DATE))>'1989' group by date_part('year', cast(release_date AS DATE)) order by date_part('year', cast(release_date AS DATE)) asc;", dbconn)
    dbcur.close()
    print("Extracted data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform visualization on the data...\n")

fig = go.Figure()
color=np.array(["rgb(100,100,100)"]*movie_count_year.shape[0])
color[movie_count_year["movie_count"].idxmax()]="rgb(255,0,0)"
fig.add_trace(go.Bar(x=movie_count_year["release_year"], y=movie_count_year["movie_count"], name="Number of movies", marker=dict(color=color.tolist())))
fig.update_layout(title="Popular movies throughout the years", xaxis_title="YEAR", yaxis_title="Number of movies", plot_bgcolor="white")
plot(fig)

# Extracting and Visualizing average popularity and vote average as per release year for released movies
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    popularity_year = sqlio.read_sql_query("SELECT avg(popularity), date_part('year', cast(release_date AS DATE)) as RELEASE_YEAR from movie_full where status = 'Released' and date_part('year', cast(release_date AS DATE))>'1959' group by date_part('year', cast(release_date AS DATE)) order by date_part('year', cast(release_date AS DATE)) asc;", dbconn)
    vote_average_year = sqlio.read_sql_query("SELECT avg(vote_average), date_part('year', cast(release_date AS DATE)) as RELEASE_YEAR from movie_full where status = 'Released' and date_part('year', cast(release_date AS DATE))>'1959' group by date_part('year', cast(release_date AS DATE)) order by date_part('year', cast(release_date AS DATE)) asc;", dbconn)
    dbcur.close()
    print("Extracted data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform visualization on the data...\n")

fig = go.Figure()
fig.add_trace(go.Scatter(name="Vote Average", x=vote_average_year["release_year"], y=vote_average_year["avg"], fill='tozeroy', fillcolor="rgb(160,160,160)", line_color="rgb(32,32,32)"))
fig.add_trace(go.Scatter(name="Popularity" ,x=popularity_year["release_year"], y=popularity_year["avg"], fill='tonexty', fillcolor="rgb(34,139,34)", line_color="rgb(0,102,0)"))
fig.update_layout(title="Trend of Movie Popularity and Vote averages", xaxis_title="YEAR", yaxis_title="POPULARITY/VOTE AVERAGE", plot_bgcolor="white")
plot(fig)

# Extracting and Visualizing average movie runtime as per release year
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    runtime_year = sqlio.read_sql_query("SELECT avg(runtime), date_part('year', cast(release_date AS DATE)) as RELEASE_YEAR from movie_full where date_part('year', cast(release_date AS DATE))>'1959' group by date_part('year', cast(release_date AS DATE)) order by date_part('year', cast(release_date AS DATE)) asc;", dbconn)
    dbcur.close()
    print("Extracted data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform visualization on the data...\n")

fig = go.Figure()
color=np.array(["rgb(100,100,100)"]*runtime_year.shape[0])
color[runtime_year["avg"].idxmax()]="rgb(255,0,0)"
fig.add_trace(go.Bar(x=runtime_year["release_year"], y=runtime_year["avg"], name="runtime of movies", marker=dict(color=color.tolist())))
fig.update_layout(title="Average runtime of movies throughout the years", xaxis_title="YEAR", yaxis_title="Average Runtime of movies", plot_bgcolor="white")
plot(fig)

# Extracting and Visualizing movie genre as per movie count
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    genre=pd.DataFrame(columns=['genre', 'count'])
    for i in range(0, len(unique_genre_names)):
        genre_each = sqlio.read_sql_query("SELECT count(mov_id) from movie_full where genre_name like '%"+unique_genre_names[i]+"%';", dbconn)
        genre = genre.append({'genre': unique_genre_names[i], 'count': genre_each["count"][0]}, ignore_index=True)
    dbcur.close()
    print("Extracted data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform visualization on the data...\n")

genre["count"] = genre["count"].astype(int)
genre = genre.sort_values('count', ascending=False)
genre = genre.reset_index(drop=True)
fig = go.Figure()
color=np.array(["rgb(100,100,100)"]*genre.shape[0])
color[genre["count"].idxmax()] = "rgb(255,0,0)"
fig.add_trace(go.Bar(x=genre["genre"], y=genre["count"], name="genre of movies", marker=dict(color=color.tolist())))
fig.update_layout(title="Spread of movies as per genres", xaxis_title="GENRE", yaxis_title="NO. OF MOVIES", plot_bgcolor="white")
plot(fig)

# Extracting and Visualizing movie production companies as per movie count
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    prod_cmpny=pd.DataFrame(columns=['prod_cmpny', 'count'])
    for i in range(0, len(unique_prod_cmpny)):
        prod_cmpny_each = sqlio.read_sql_query("SELECT count(mov_id) from movie_full where prod_cmpny like $$%"+unique_prod_cmpny[i]+"%$$;", dbconn)
        prod_cmpny = prod_cmpny.append({'prod_cmpny': unique_prod_cmpny[i], 'count': prod_cmpny_each["count"][0]}, ignore_index=True)
    dbcur.close()
    print("Extracted data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform visualization on the data...\n")

prod_cmpny["count"] = prod_cmpny["count"].astype(int)
prod_cmpny = prod_cmpny.sort_values('count', ascending=False)
prod_cmpny = prod_cmpny.reset_index(drop=True)
fig = go.Figure()
color=np.array(["rgb(100,100,100)"]*prod_cmpny.shape[0])
color[prod_cmpny["count"].idxmax()] = "rgb(255,0,0)"
fig.add_trace(go.Bar(x=prod_cmpny["prod_cmpny"][:30], y=prod_cmpny["count"][:30], name="production company of movies", marker=dict(color=color.tolist())))
fig.update_layout(title="Movie count as per production companies", xaxis_title="PRODUCTION COMPANY", yaxis_title="NO. OF MOVIES", plot_bgcolor="white")
plot(fig)

# Extracting and Visualizing movie production countries as per movie count
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    prod_cntry=pd.DataFrame(columns=['prod_cntry', 'count'])
    for i in range(0, len(unique_prod_cntry)):
        prod_cntry_each = sqlio.read_sql_query("SELECT count(mov_id) from movie_full where prod_cntry like $$%"+unique_prod_cntry[i]+"%$$;", dbconn)
        prod_cntry = prod_cntry.append({'prod_cntry': unique_prod_cntry[i], 'count': prod_cntry_each["count"][0]}, ignore_index=True)
    dbcur.close()
    print("Extracted data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform visualization on the data...\n")

prod_cntry["count"] = prod_cntry["count"].astype(int)
prod_cntry = prod_cntry.sort_values('count', ascending=False)
prod_cntry = prod_cntry.reset_index(drop=True)
fig = go.Figure()
color=np.array(["rgb(100,100,100)"]*prod_cntry.shape[0])
color[prod_cntry["count"].idxmax()] = "rgb(255,0,0)"
color[1] = "rgb(0,0,255)"
fig.add_trace(go.Bar(x=prod_cntry["prod_cntry"][:20], y=prod_cntry["count"][:20], name="production country of movies", marker=dict(color=color.tolist())))
fig.update_layout(title="Movie count as per production countries", xaxis_title="PRODUCTION COUNTRY", yaxis_title="NO. OF MOVIES", plot_bgcolor="white")
plot(fig)

# Extracting and Visualizing movie languages as per movie count
try:
    dbconn = pc2.connect(
        user="dapgrpl",
        password="dapgrpl",
        host="18.203.68.115",
        port="5432",
        database="tmdb")
    dbcur = dbconn.cursor()
    mov_lang=pd.DataFrame(columns=['language', 'count'])
    for i in range(0, len(unique_languages)):
        mov_lang_each = sqlio.read_sql_query("SELECT count(mov_id) from movie_full where languages like '%"+unique_languages[i]+"%';", dbconn)
        mov_lang = mov_lang.append({'language': unique_languages[i], 'count': mov_lang_each["count"][0]}, ignore_index=True)
    dbcur.close()
    print("Extracted data from postgres for visualizations.\n")
except (Exception, pc2.Error) as dbError:
    print("Error while connecting to PostgreSQL\n", dbError)
finally:
    print("Proceeding to perform visualization on the data...\n")

mov_lang["count"] = mov_lang["count"].astype(int)
mov_lang = mov_lang.sort_values('count', ascending=False)
mov_lang = mov_lang.reset_index(drop=True)
fig = go.Figure()
color=np.array(["rgb(100,100,100)"]*mov_lang.shape[0])
color[mov_lang["count"].idxmax()] = "rgb(255,0,0)"
color[1] = "rgb(0,0,255)"
fig.add_trace(go.Bar(x=mov_lang["language"][:20], y=mov_lang["count"][:20], name="languages of movies", marker=dict(color=color.tolist())))
fig.update_layout(title="Trend of movie count as per languages", xaxis_title="LANGUAGE", yaxis_title="NO. OF MOVIES", plot_bgcolor="white")
plot(fig)

# Deleting obsolete variables.
del mov_list, color, dbconn, dbcur, genre_each, fig, mongo_id, mongo_ins, mov_lang_each, movie_all_mng, prod_cmpny_each, prod_cntry_each, response, tmdb,  i, unique_cmpny, unique_cntry, unique_genres, unique_lang, dbexists, sql, adult, a, b, budget, cmp, cntr, collname, genre, genre_name, lang, languages, mov_id, movie_list, popularity, prod_cmpny, prod_cntry, release_date, revenue, runtime, status, title, vote_average, vote_count
