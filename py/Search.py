from qdrant_client import models, QdrantClient
from sentence_transformers import SentenceTransformer
from DB_connect import createConnection
import pandas as pd
import re
import numpy as np
from multiprocessing import Pool, cpu_count
import itertools

model = None

def initWorker():
   global model
   model = SentenceTransformer('keepitreal/vietnamese-sbert')

def getDocuments(limit = 1):
  connection = createConnection()
  QUERY = f"SELECT * FROM products LIMIT {limit};"
  df = pd.read_sql(QUERY, connection)
  print("Get document successfully.")
  return df.to_dict(orient="records")

def modifyText(text):
  clean_text = text.replace('\xa0', ' ')
  clean_text = re.sub(r"[^\w\sÀ-Ỵà-ỵ]", " ", clean_text)
  clean_text = re.sub(r"_", " ", clean_text)
  clean_text = re.sub(r"\s+", " ", clean_text)
  clean_text = clean_text.strip().lower()
  return clean_text.split()

def createCollection(client, model, collectionName):
  client.create_collection(
    collection_name=collectionName,
    vectors_config=models.VectorParams(
        size=model.get_sentence_embedding_dimension(),
        distance=models.Distance.COSINE,
    ),
  )
  print("Create collection successfully.")
  return None

def processProduct(args):
  product, base_id = args
  try:
    name = product.get("name", "")
    desc = product.get("description", "")
    wordList = modifyText(desc)
    nameEncode = model.encode(name)
    phrases = [" ".join(wordList[i:i+5]) for i in range(len(wordList) - 4)]

    if not phrases:
      return []

    pharaseEmbeddings = model.encode(phrases, batch_size=256, show_progress_bar=False)
    embeddings = [nameEncode]
    for emb in pharaseEmbeddings:
      embeddings.append(emb)

    marked = [False] * len(embeddings)
    for i in range(len(embeddings)):
      if (not marked[i]):
        for k in range(i + 1, len(embeddings)):
          if (not marked[k]):
            sim = np.dot(embeddings[i], embeddings[k]) / (np.linalg.norm(embeddings[i]) * np.linalg.norm(embeddings[k]) + 1e-10)
            if sim > 0.9:
              marked[k] = True

    points = []
    for i, emb in enumerate(embeddings):
      if (not marked[i]):
        points.append(
          models.PointStruct(
          id=base_id + i,
          vector=emb.tolist(),
          payload={"name": name}
          )
        )
    return points
  except Exception as e:
      print(f"Error processing product: {e}")
      return []

def updateCollection(client, model, documents, collectionName):
  poolSize = 10
  print("Processing products in parallel...")
  args = [(product, i * 10000) for i, product in enumerate(documents)]

  with Pool(poolSize, initializer = initWorker) as pool:
    allPointsNested = pool.map(processProduct, args)

  allPoints = list(itertools.chain.from_iterable(allPointsNested))
  print(f"Uploading {len(allPoints)} vectors to Qdrant...")

  for i in range(0, len(allPoints), 100):
    batch = allPoints[i:i + 100]
    client.upload_points(collection_name=collectionName, points=batch)

  print("Update collection successfully.")

def main():
  localModel = SentenceTransformer('keepitreal/vietnamese-sbert')
  documents = getDocuments(100)
  # print(documents)
  client = QdrantClient(":memory:")
  createCollection(client, localModel, "products")
  updateCollection(client, localModel, documents, "products")
  info = input("Search product: ")
  while (not info == ""):
    hits = client.query_points(
      collection_name="products",
      query=localModel.encode(info).tolist(),
      limit=10,
    ).points
    printed_names = set()
    for hit in hits:
      name = hit.payload.get("name")
      if name not in printed_names:
        print(f"Product: {name}, score: {hit.score}")
        printed_names.add(name)
          
    info = input("Search product: ")

if __name__ == "__main__":
  main()