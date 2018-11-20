import pymongo

from bson.objectid import ObjectId
from datetime import datetime
from pprint import pprint

# This steps assume that a MongoDB instance is running on the default host and port. (host= "localhost", port= 27017)
# Otherwise, we show previously start a mongoDB instance from bash, using:
# > sudo service mongod start

#########################
# CONNECTION
#########################

# The first step when working with PyMongo is to create a MongoClient to the running mongod instance. 

# with default host and port
myclient = pymongo.MongoClient() 
# the `host` parameter can be a full URI
myclient2 = pymongo.MongoClient(host="mongodb://localhost:27017/")
# giving both host and port
myclient3 = pymongo.MongoClient(host= "localhost", port= 27017)

#########################
# DB & COLLECTIONS
#########################

# A single instance of MongoDB can support multiple independent databases.

# Look for databases:
pprint("Mongo actual databases:")
pprint(myclient.database_names())

# Look for database or create a new one
mydb = myclient["mydatabase"]

# A collection is a group of documents stored in MongoDB,
# and can be thought of as roughly the equivalent of a table in a relational database.
# Look for collections:
pprint("Mongo actual collecions:")
pprint(mydb.collection_names())

# Look for collection or create a new one
mycol = mydb["customers"]

#########################
# INSERT
#########################

# Insert one element
# Note that documents can contain native Python types (like datetime.datetime instances)
# which will be automatically converted to and from the appropriate BSON types.
print("Inserting element...")
elem = {"name": "Fede", "address": "Sideway 1633", "date": datetime.utcnow()}

# When a document is inserted a special key,
# "_id", is a spetial BSON type and is automatically added if the document doesn’t already contain an "_id" key.
# The value of "_id" must be unique across the collection.
id_elem = mycol.insert_one(elem)
print("ID of element inserted:", id_elem.inserted_id)

# After inserting the first document, the posts collection has actually been created on the server.
# We can verify this by listing all of the collections in our database:
print("Veryfiying collections...")
pprint(mydb.collection_names(include_system_collections=False))

# Insert multiple elements
print("Inserting a list...")
mylist = [
  { "name": "John", "address": "Highway 37"},
  { "name": "Peter", "address": "Lowstreet 27"},
  { "name": "Amy", "address": "Apple st 652"},
  { "name": "Hannah", "address": "Mountain 21"},
  { "name": "Michael", "address": "Valley 345"},
  { "name": "Sandy", "address": "Ocean blvd 2"},
  { "name": "Betty", "address": "Green Grass 1"},
  { "name": "Richard", "address": "Sky st 331"},
  { "name": "Susan", "address": "One way 98"},
  { "name": "Vicky", "address": "Yellow Garden 2"},
  { "name": "Ben", "address": "Park Lane 38"},
  { "name": "William", "address": "Central st 954"},
  { "name": "Chuck", "address": "Main Road 989"},
  { "name": "Viola", "address": "Sideway 1633"}
]
id_list = mycol.insert_many(mylist)
print("ID of elements inserted:")
pprint(id_list.inserted_ids)

#########################
# SIMPLE QUERIES
#########################

# The most basic type of query that can be performed in MongoDB is find_one()

# find first element
x = mycol.find_one()
print("First element found in my collection:")
pprint(x)

# find_one() also supports querying on specific elements that the resulting document must match.
# To limit our results to a document with "_id" we do:
# (we must provide and ObjectId as argument)
print("Elemnt found with a given _id:")
pprint(mycol.find_one({"_id": ObjectId(id_elem.inserted_id)}))

# returns all occurrences in the selection.
print("All elements found in my collection:")
for x in mycol.find():
    pprint(x)

# Counting documents
print("Amount of documents in my collection", mycol.count_documents({}))
# Counting just those documents that match a specific query:
print("Amount of documents in my collection with name:Fede", mycol.count_documents({"name":"Fede"}))

#########################
# RANGE QUERIES
#########################

# Spetial queries
print("Posts with date 'lt' lower than d, and also sorted by name")
d = datetime.utcnow()
for post in mycol.find({"date": {"$lt": d}}).sort("name"):
  pprint(post)

#########################
# INDEXING
#########################

# Indexing to accelerate certain queries

# Let's create a unique index on a key that rejects documents whose value for that key already exists in the index.
# First, we’ll need to create the index on a new collection:
result = mydb.mynewcol.create_index([('user_id', pymongo.ASCENDING)], unique=True)
print("Indexes on my database:")
pprint(sorted(list(mydb.mynewcol.index_information())))

# We have two indexes now: one is the index on _id that MongoDB creates automatically,
# and the other is the index on user_id we just created.

# Set up some user profiles:
print("Inserting entries with 'user_id' keys")
user_profiles = [
     {'user_id': 211, 'name': 'Luke'},
     {'user_id': 212, 'name': 'Ziltoid'}]
result = mydb.mynewcol.insert_many(user_profiles)

print("Looking for entries with custom 'user_id'")
pprint(mydb.mynewcol.find_one({"user_id": 211}))

# The index prevents us from inserting a document whose user_id is already in the collection:
print("Trying to insert a new entry with same user_id")
duplicate_profile = {'user_id': 211, 'name': 'Tommy'}
try:
  result = mydb.mynewcol.insert_one(duplicate_profile)
except Exception as e:
  print(e)

#########################
# DROP
#########################

# Empty databases
print("Dropping collections:")
mydb.mycol.drop()
mydb.mynewcol.drop()
print("Looking for collections:")
pprint(mydb.collection_names())