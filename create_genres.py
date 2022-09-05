import pandas as pd
import Neo4jConnection
import os
from dotenv import load_dotenv

load_dotenv()

movies_raw_fname = "raw/tmdb_5000_movies.csv"

movies_df = pd.read_csv(movies_raw_fname)
genre_dfs = []

conn = Neo4jConnection.Connection(
  uri=os.getenv('NEO4J_DB_URI'), 
  user=os.getenv('NEO4J_DB_USER'), 
  pwd=os.getenv('NEO4J_DB_PWD')
)
  
query = "CREATE (genre:GENRE {id: $id, name: $name})"

for _, row in movies_df.iterrows():
  cast = pd.read_json(row["genres"])
  genre_dfs.append(cast)

merged_df = pd.concat(genre_dfs)
unique_merged_df = merged_df.drop_duplicates(subset="id")

print(merged_df)
print(unique_merged_df)

for _, row in unique_merged_df.iterrows():
  params = {
    "id": row["id"],
    "name": row["name"]
  }
  conn.query(query, params)

