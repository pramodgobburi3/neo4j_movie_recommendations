import pandas as pd
import Neo4jConnection
import os
from dotenv import load_dotenv

load_dotenv()

movies_raw_fname = "raw/tmdb_5000_movies.csv"
credits_raw_fname = "raw/tmdb_5000_credits.csv"

conn = Neo4jConnection.Connection(
  uri=os.getenv('NEO4J_DB_URI'), 
  user=os.getenv('NEO4J_DB_USER'), 
  pwd=os.getenv('NEO4J_DB_PWD')
)

bulk_query = """
UNWIND $movies as movie
CREATE (m:MOVIE {
  id: movie.id,
  title: movie.title,
  overview: movie.overview,
  releaseDate: movie.releaseDate,
  rating: movie.rating
})
WITH movie
UNWIND movie.cast as actor
MATCH (a: ACTOR {id: actor.id}),(m: MOVIE {id: movie.id})
CREATE (a) -[:ACTED_IN {character: actor.character}]-> (m)
CREATE (m) -[:CASTED]-> (a)
WITH DISTINCT movie
UNWIND movie.genres as genre
MATCH (g: GENRE {id: genre.id}),(m: MOVIE {id: movie.id})
CREATE (g) -[:CONTAINS]-> (m)
CREATE (m) -[:BELONGS_TO]-> (g)
"""

create_movie_query = """
CREATE (movie:MOVIE {
  id: $id,
  title: $title,
  overview: $overview,
  releaseDate: $releaseDate,
  rating: $rating
})
"""

create_cast_rel_query = """
UNWIND $actors as actor
MATCH (a: ACTOR {id: actor.id}),(m: MOVIE {id: $movie_id})
CREATE (a) -[:ACTED_IN {character: actor.character}]-> (m)
CREATE (m) -[:CASTED]-> (a)
"""

create_genre_rel_query = """
UNWIND $genres as genre
MATCH (g: GENRE {id: genre.id}),(m: MOVIE {id: $movie_id})
CREATE (g) -[:CONTAINS]-> (m)
CREATE (m) -[:BELONGS_TO]-> (g)
"""

movies_df = pd.read_csv(movies_raw_fname)
credits_df = pd.read_csv(credits_raw_fname)

movies = []

def find_cast(movie_id):
  for _, row in credits_df.iterrows():
    if (row["movie_id"] == movie_id):
      return pd.read_json(row["cast"]).to_dict('records')
  return None

def build_movie_obj(raw_movie, cast):
  movie = {}
  movie["id"] = raw_movie["id"]
  movie["title"] = raw_movie["title"]
  movie["genres"] = pd.read_json(raw_movie["genres"]).to_dict('records')
  movie["cast"] = cast
  movie["keywords"] = raw_movie["keywords"]
  movie["overview"] = raw_movie["overview"]
  movie["releaseDate"] = raw_movie["release_date"]
  movie["rating"] = raw_movie["vote_average"]
  return movie

def add_cast_relations(movie):
  params = { "actors": movie["cast"], "movie_id": movie["id"] }
  conn.query(create_cast_rel_query, params)

def add_genre_relations(movie):
  params = { "genres": movie["genres"], "movie_id": movie["id"] }
  conn.query(create_genre_rel_query, params)

i = 0
for col_name, row in movies_df.iterrows():
  i += 1
  movie_id = row["id"]
  cast = find_cast(movie_id)
  if cast == None:
    continue
  else:
    movie = build_movie_obj(row, cast)
    movies.append(movie)
    print("added " + str(i) + " of " + str(movies_df.shape[0]))
    # conn.query(create_movie_query, movie)
    # add_cast_relations(movie)
    # add_genre_relations(movie)
print("built movies")
conn.query(bulk_query, { "movies": movies })
print("completed")


