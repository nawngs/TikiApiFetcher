import json
import urllib.error
import pandas as pd
import time
import urllib.request
from bs4 import BeautifulSoup
from multiprocessing import Pool, cpu_count
import numpy as np

productIDCSV = "db/ProductIDs.csv"
mainURL = "https://api.tiki.vn/product-detail/api/v1/products/"

df = pd.read_csv(productIDCSV)
ids = df["id"].values
np.random.shuffle(ids)
# ids2 = [ids[i] for i in range(0, 1000)]
# ids = ids2

for code in ["404", "429"]:
  with open(f"db/{code}.csv", "w", newline='', encoding='utf-8') as f:
    f.write("id\n")  # optional header

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
  URL = mainURL + str(productID)
  request = urllib.request.Request(
    URL,
    headers={"User-Agent": "Mozilla/5.0"}
  )

  try:
    with urllib.request.urlopen(request, timeout=10) as response:
      data = json.load(response)
      print(f"Fetching product {productID} successfully.")
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

def scrape():
  global ids
  poolSize = min(cpu_count(), 4)
  firstAttempt = True
  for attempt in range(1, 101):
    with open("db/429.csv", "w", newline='', encoding='utf-8') as f:
      f.write("id\n")
    print(f"Attempt number {attempt}.")
    with Pool(poolSize) as pool:
      results = pool.map(fetchPage, ids)

    valid_results = [r for r in results if r is not None]
    df_results = pd.DataFrame(valid_results)
    df_results.to_csv(
        "db/ProductInfo.csv",
        mode='a' if not firstAttempt else 'w',
        header=firstAttempt,
        index=False
    )
    firstAttempt = False

    df = pd.read_csv("db/429.csv")
    ids = df["id"].values
    np.random.shuffle(ids)
    
    if (len(ids) == 0):
      break
    
    time.sleep(attempt * 2)

  print("Finish scrapping.")

if __name__ == "__main__":
    scrape()