# items_fixed.py
import os
import random
import uuid
import time
import json
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError, DuplicateKeyError, OperationFailure

load_dotenv()

CONN_STRING = os.getenv("COSMOS_CONN_STRING")
DB_NAME = os.getenv("DB_NAME")
COLL_NAME = os.getenv("COLLECTION")

if not CONN_STRING or not DB_NAME or not COLL_NAME:
    raise SystemExit("Please set COSMOS_CONN_STRING, DB_NAME and COLLECTION in your .env file")

client = MongoClient(CONN_STRING, tls=True, tlsAllowInvalidCertificates=True)
db = client[DB_NAME]
collection = db[COLL_NAME]

# Categories and price ranges
CATEGORIES = {
    "Electronics": (["Laptop", "Smartphone", "Mouse", "Keyboard", "Headphones", "Monitor"], (1000.0, 150000.0)),
    "Soft Toys": (["Teddy Bear", "Panda Plush", "Bunny Plush", "Fox Plush"], (200.0, 3000.0)),
    "Books": (["Novel", "Comics", "Notebook", "Diary", "Guidebook"], (50.0, 1500.0)),
    "Groceries": (["Rice Bag", "Oil Bottle", "Sugar Pack", "Milk Packet", "Flour Pack"], (20.0, 3000.0)),
    "Fashion": (["T-Shirt", "Jeans", "Shoes", "Cap", "Dress"], (150.0, 8000.0)),
    "Sports": (["Dumbbells", "Yoga Mat", "Treadmill", "Skipping Rope"], (100.0, 80000.0))
}

def _random_price_for_category(cat):
    rng = CATEGORIES[cat][1]
    return round(random.uniform(rng[0], rng[1]), 2)

def generate_products(n=100, null_count=10):
    if null_count > n:
        raise ValueError("null_count cannot be greater than n")

    products = []
    null_indices = set(random.sample(range(n), null_count))
    cats = list(CATEGORIES.keys())

    for i in range(n):
        cat = random.choice(cats)
        name_base = random.choice(CATEGORIES[cat][0])
        variant = random.choice(["A", "B", "Pro", "X", "2024", "Lite", "S"])
        name = f"{name_base} {variant}"

        price = None if i in null_indices else _random_price_for_category(cat)

        item = {
            "_id": str(uuid.uuid4()),
            "name": name,
            "category": cat,
            "price": price,
            "inStock": random.choice([True, True, True, False]),
            "meta": {"seeded_by": "items_fixed.py", "index": i}
        }
        products.append(item)
    return products

# Robust insertion logic
def insert_in_batches_with_retries(products,
                                    batch_size=10,
                                    max_batch_retries=3,
                                    max_doc_retries=3,
                                    base_backoff=1.0):
    inserted_count = 0
    failed_docs = []

    for start in range(0, len(products), batch_size):
        batch = products[start:start+batch_size]
        batch_number = start // batch_size + 1

        attempt = 0
        while attempt <= max_batch_retries:
            try:
                result = collection.insert_many(batch, ordered=False)
                inserted_count += len(result.inserted_ids)
                print(f"[Batch {batch_number}] Inserted {len(result.inserted_ids)} items.")
                break  # success for this batch
            except BulkWriteError as bwe:
                attempt += 1
                write_errors = bwe.details.get('writeErrors', [])
                print(f"[Batch {batch_number}] BulkWriteError on attempt {attempt}: {len(write_errors)} writeErrors. Retrying batch after backoff.")
                # exponential backoff
                time.sleep(base_backoff * (2 ** (attempt-1)))
                if attempt > max_batch_retries:
                    # fallback to per-document insert with retries
                    print(f"[Batch {batch_number}] Batch retry limit reached. Falling back to single-document inserts.")
                    for doc in batch:
                        success = try_insert_single(doc, max_retries=max_doc_retries, base_backoff=base_backoff)
                        if success:
                            inserted_count += 1
                        else:
                            failed_docs.append(doc)
                    break
            except (OperationFailure, PyMongoError) as e:
                attempt += 1
                print(f"[Batch {batch_number}] PyMongo error on attempt {attempt}: {e}. Retrying after backoff.")
                time.sleep(base_backoff * (2 ** (attempt-1)))
                if attempt > max_batch_retries:
                    print(f"[Batch {batch_number}] Batch retry limit reached due to errors. Falling back to single-document inserts.")
                    for doc in batch:
                        success = try_insert_single(doc, max_retries=max_doc_retries, base_backoff=base_backoff)
                        if success:
                            inserted_count += 1
                        else:
                            failed_docs.append(doc)
                    break
            except Exception as e:
                print(f"[Batch {batch_number}] Unexpected error: {e}. Falling back to single-document inserts.")
                for doc in batch:
                    success = try_insert_single(doc, max_retries=max_doc_retries, base_backoff=base_backoff)
                    if success:
                        inserted_count += 1
                    else:
                        failed_docs.append(doc)
                break

    return inserted_count, failed_docs

def try_insert_single(doc, max_retries=3, base_backoff=1.0):
    attempt = 0
    while attempt < max_retries:
        try:
            collection.insert_one(doc)
            return True
        except DuplicateKeyError:
            # If duplicate key, try replace_one upsert to ensure the doc exists
            try:
                collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
                return True
            except Exception as e:
                attempt += 1
                time.sleep(base_backoff * (2 ** attempt))
        except BulkWriteError as bwe:
            attempt += 1
            time.sleep(base_backoff * (2 ** attempt))
        except (OperationFailure, PyMongoError) as e:
            attempt += 1
            print(f"  Single insert error (attempt {attempt}) for {_short_id(doc)}: {str(e)}. Backing off.")
            time.sleep(base_backoff * (2 ** attempt))
        except Exception as e:
            attempt += 1
            print(f"  Unexpected single-insert error (attempt {attempt}) for {_short_id(doc)}: {e}")
            time.sleep(base_backoff * (2 ** attempt))

    # final fallback: try upsert once
    try:
        collection.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        return True
    except Exception as e:
        print(f"  Final upsert failed for {_short_id(doc)}: {e}")
        return False

def _short_id(doc):
    return doc.get("_id", "")[:8]

def count_and_show_sample(flag="items_fixed.py"):
    total = collection.count_documents({"meta.seeded_by": flag})
    nulls = collection.count_documents({"meta.seeded_by": flag, "price": None})
    some_docs = list(collection.find({"meta.seeded_by": flag}).limit(10))
    print(f"Total seeded docs (meta.seeded_by='{flag}'): {total}")
    print(f"Docs with price = null among seeded docs: {nulls}")
    print("Sample docs:")
    for d in some_docs:
        print(d)

if __name__ == "__main__":
    N = 100
    NULL_COUNT = 10
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
    MAX_BATCH_RETRIES = int(os.getenv("MAX_BATCH_RETRIES", "3"))
    MAX_DOC_RETRIES = int(os.getenv("MAX_DOC_RETRIES", "3"))
    BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "1.0"))

    print("Generating products...")
    products = generate_products(n=N, null_count=NULL_COUNT)
    print(f"Attempting to insert {len(products)} products in batches of {BATCH_SIZE} ...")

    inserted, failed = insert_in_batches_with_retries(
        products,
        batch_size=BATCH_SIZE,
        max_batch_retries=MAX_BATCH_RETRIES,
        max_doc_retries=MAX_DOC_RETRIES,
        base_backoff=BASE_BACKOFF
    )

    print("---------------")
    print("Insert Summary:")
    print("Inserted:", inserted)
    print("Failed:", len(failed))
    print("---------------")

    if failed:
        # persist failed docs for inspection / manual retry
        with open("failed_inserts.json", "w", encoding="utf-8") as f:
            json.dump(failed, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(failed)} failed documents to failed_inserts.json")

    print("Verifying counts from DB...")
    count_and_show_sample()


