import json
import urllib.error
import pandas as pd
import time
import urllib.request
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count
import numpy as np
import random

from Load import Load

mainURL = "https://api.tiki.vn/product-detail/api/v1/products/"

def htmlToText(htmlString):
  if not htmlString:
    return ""
  soup = BeautifulSoup(htmlString, "html.parser")
  text = soup.get_text(separator="\n")
  return text.replace('\n', ' ').strip()

def logError(productID, errorType):
  logfile = f"db/{errorType}.csv"
  with open(logfile, mode='a', newline='', encoding='utf-8') as f:
    f.write(f"{productID}\n")
   

def fetchPage(productID):
  # df_done = pd.read_csv("db/Success.csv")
  # ids_done = set(df_done["id"].values)
  # if (productID in ids_done):
  #   return None
    
  URL = mainURL + str(productID)

  request = urllib.request.Request(
    URL,
    headers={"User-Agent": "Mozilla/5.0"}
  )

  try:
    with urllib.request.urlopen(request) as response:
      data = json.load(response)
      print(f"Fetching product {productID} successfully.")
      time.sleep(random.uniform(1, 2))
  except urllib.error.URLError as e:
    print(f"Fetching product {productID} error: {e}")
    if (e.code == 429):
      logError(productID, "429")
    elif (e.code == 404):
      logError(productID, "404")
    return None
  except Exception as e:
    print(f"Unexpected error fetching product {productID}: {e}")
    return None

  return {
    "productID": data.get("id"),
    "productName": data.get("name"),
    "productPrice": data.get("price"),
    "productDescription": htmlToText(data.get("description"))
  }

def scrape(ids_batch):
  poolSize = 10

  for attempt in range(1, 4):
    with open("db/429.csv", "w", newline='', encoding='utf-8') as f:
      f.write("id\n")

    print(f"Attempt number {attempt}.")
    with Pool(poolSize) as pool:
      results = pool.map(fetchPage, ids_batch)

    valid_results = [r for r in results if r is not None]

    df429 = pd.read_csv("db/429.csv")
    ids_batch = df429["id"].values
    np.random.shuffle(ids_batch)
    
    if (len(ids_batch) == 0):
      break
    
    time.sleep(1)

  print("Finish scrapping.")
  return valid_results

if __name__ == "__main__":
  df = pd.read_csv("db/ProductIDs.csv")
  try:
    df_done = pd.read_csv("db/Success.csv")
    ids_done = set(df_done["id"].values)
    ids = [id for id in df['id'].values if id not in ids_done]
  except:
    ids = df['id'].values

  ids = list(set(ids))
  np.random.shuffle(ids)

  for code in ["404", "429"]:
    with open(f"db/{code}.csv", "w", newline='', encoding='utf-8') as f:
      f.write("id\n")  # optional header

  NumBatch = 100
  BatchSize = len(ids) // NumBatch
  for i in range(0, len(ids), BatchSize):
    j = min(i + BatchSize -1, len(ids) - 1)
    Products = scrape(ids[i:j+1])
    with open("db/Success.csv", "a") as f:
      for id_done in ids[i:j+1]:
        f.write(str(id_done) + '\n')
    Load(Products)