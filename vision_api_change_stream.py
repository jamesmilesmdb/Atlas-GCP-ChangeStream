import pymongo
from bson.objectid import ObjectId
from bson.json_util import dumps
from google.cloud import vision
import dns
import json
import os
import sys
import logging
print ("Import successful!")

URI = "<YOUR_CONNECTION_STRING>" #Replace <YOUR_CONNECTION_STRING> with the connection string of your Atlas cluster
DBNAME = "<ENTER_DB_NAME>" #Replace "<ENTER_DB_NAME>" with "sample_mflix"
COLL = "<ENTER_COLLECTION_NAME>" #Replace "<ENTER_COLLECTION_NAME>""="movies"
conn = pymongo.MongoClient(URI)

try:
    conn.server_info()
except Exception as e:
    logging.error("Unable to connect to {s}".format(s=URI))
    conn = None
    sys.exit(1)

handle = conn[DBNAME][COLL]
print("Connected!")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="adc.json"
print("Credentials Loaded!")

try:
    gcpapi = vision.ImageAnnotatorClient()
    print("Vision API Instance Created!")
except Exception as e:
    print(e)
    sys.exit(1)

# connect to a change stream
change_stream = handle.watch()
print("WAITING FOR A CHANGE\n\n\n\n")
# every change in the db
for change in change_stream:
    print("WAITING FOR A CHANGE\n\n\n\n")
    # can be insert, update, replace (Compass)
    if change["operationType"] == "insert":
        # make sure it had a URL attribute
        if "poster" in change["fullDocument"]:
            # boilerplate to prep gcp api request
            print("A CHANGE OCCURED \n\n\n")
            image = vision.Image()
            image.source.image_uri = change["fullDocument"]["poster"]
            resp = gcpapi.label_detection(image=image)
            print(resp)
            # If image does not exist then, send message on co
            if resp.error.code == 0 :
            # odd formatting i dont have time for right now so just process it first
                labels = []
                for label in resp.label_annotations:
                    obj = {}
                    obj['description'] = label.description
                    obj['score'] = label.score
                    labels.append(obj)
                    # update mongodb record with response from GCP
                handle.update_one({'_id':ObjectId(change["fullDocument"]["_id"])}, {"$set": {"gcpvisionlabels":labels}})
            else:
                logging.warning("API error on URl: {a}".format(a=resp.error.message))
                logging.warning(change["fullDocument"]["poster"])
    