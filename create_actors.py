import pandas as pd
import Neo4jConnection
import os
from dotenv import load_dotenv

load_dotenv()

credits_raw_fname = "raw/tmdb_5000_credits.csv"

credits_df = pd.read_csv(credits_raw_fname)
actor_dfs = []

conn = Neo4jConnection.Connection(
  uri=os.getenv('NEO4J_DB_URI'), 
  user=os.getenv('NEO4J_DB_USER'), 
  pwd=os.getenv('NEO4J_DB_PWD')
)

query = "CREATE (actor:ACTOR {id: $id, name: $name, gender: $gender})"

for _, row in credits_df.iterrows():
  cast = pd.read_json(row["cast"])
  actor_dfs.append(cast)

merged_df = pd.concat(actor_dfs)
unique_merged_df = merged_df.drop_duplicates(subset="id")

for _, row in unique_merged_df.iterrows():
  params = {
    "id": row["id"],
    "name": row["name"],
    "gender": "female" if row["gender"] == 1 else "male"
  }
  conn.query(query, params)

