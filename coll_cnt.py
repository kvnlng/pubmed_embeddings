import chromadb
from chromadb import Settings

client = chromadb.PersistentClient(path="/data/vector/chromadb", settings=Settings())
collection = client.get_collection(name="pubmed_texts")
print(collection.count())
