from flask import Flask, request, jsonify, make_response
import os
import rediscluster
import pymongo
import json
import time
import random
from bson import json_util

#redis connect
def connect():
    startup_nodes = [{ "host": os.environ.get("FLASK_REDIS_HOST"), "port": os.environ.get("FLASK_REDIS_PORT")}]
    redis_pool = rediscluster.ClusterConnectionPool(max_connections=5, startup_nodes=startup_nodes, skip_full_coverage_check=True, decode_responses=False)
    return redis_pool
r = rediscluster.RedisCluster(connection_pool=connect())

#docdb connect
auth_string = os.environ.get("DOCUMENTDB_USER") + ":" + os.environ.get("DOCUMENTDB_PASS") + "@" + os.environ.get("DOCUMENTDB_HOST") + ":27017/" 
flags = "?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
docdb_client = pymongo.MongoClient('mongodb://' + auth_string + flags ) 
docdb_database = docdb_client["mydatabase"]
docdb_collection = docdb_database['movies']

app = Flask(__name__)

@app.route('/flush')
def flush():
    return str(r.flushall())

@app.route('/getCollections')
def getCollections():
    resp = make_response(jsonify(docdb_database.collection_names(include_system_collections=False)))
    resp.headers['Content-Type'] = "application/json"
    return resp
    
@app.route('/findOne/<collection>')
def findOne(collection):
    docdb_collection = docdb_database[collection]
    resp = make_response(json_util.dumps(docdb_collection.find().limit(1).skip(random.randint(1, 1000))))
    resp.headers['Content-Type'] = "application/json"
    return resp 


@app.route('/search/<collection>/<column_type>/<value>')
def searchCollections(collection, column_type, value, cache_hit="yes", cache_end_time=0, db_end_time=0):
    docdb_collection = docdb_database[collection]
    key = "search:" + collection + ":" + column_type + ":" + value 
    if (r.exists(key) > 0):
        cache_start_time = time.perf_counter()
        redis_data = redis_get_key(r, key)
        cache_end_time = time.perf_counter() - cache_start_time
        resp = make_response(redis_data)
        resp = make_header(resp, cache_hit, cache_end_time, db_end_time)
        return resp
    else:
        query = {column_type : {"$regex": "^" + value}}
        ddb_data, docdb_end_time = search_collection_from_ddb(docdb_collection, query)
        cache_object(key,ddb_data)
        return searchCollections(collection, column_type, value, cache_hit="no", cache_end_time=0, db_end_time=docdb_end_time)

@app.route('/searchDocDB/<collection>/<column_type>/<value>')
def searchDocDB(collection, column_type, value):
    docdb_collection = docdb_database[collection]
    query = {column_type : value}
    stored_data, docdb_end_time = search_collection_from_ddb(docdb_collection, query)
    resp = make_response(stored_data)
    resp = make_header(resp, "No", 0, docdb_end_time)
    return resp
        
def redis_get_key(r, key):
    redis_data = r.get(key)
    return redis_data
    
def make_header(resp, cache_hit, cache_end_time, docdb_end_time):
    resp.headers['Content-Type'] = "application/json"
    resp.headers['Cache-Hit'] = cache_hit
    resp.headers['Cache-Time-Seconds'] =  cache_end_time
    resp.headers['DB-Time-Seconds'] = docdb_end_time 
    return resp
    
def search_collection_from_ddb(docdb_collection, query):
    db_start_time = time.perf_counter()
    response = docdb_collection.find(query)
    stored_data = json_util.dumps(list(response))
    docdb_end_time = time.perf_counter() - db_start_time
    return stored_data, docdb_end_time
    
def cache_object(key, data):
    return r.set(key, data)
  

if __name__ == "__main__":
    app.logger.debug('Starting App')
    app.run(host ='0.0.0.0', port = 5000, debug = True) 
    
    
