from pymongo import MongoClient

def url_in_db(url):
    url=str(url).replace("."," ")
    db = MongoClient('127.0.0.1', 27017).duzhe
    collection = db.url
    result = collection.find_one({"url":url})
    print(result)
    if(result==None):
        collection.insert({"url": url})
        return True
    else:
        return False

print(url_in_db("hha"))