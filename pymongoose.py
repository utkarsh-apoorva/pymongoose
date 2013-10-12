

	#################################################################################################
	#                                                                                               #
	#  This library allows one to create a MongoDB Schema and perform all basic Database queries    #
	#  It is written on top of PyMongo Connector                                                    #
	#                                                                                               #
	#                                                                                               #
	#                                                                                               #
	#                                                                                               #
	#  Author: Utkarsh Apoorva                                                                      #
	#  Homepage: www.utkarshapoorva.com                                                             #
	#  @License: The MIT License (MIT)                                                              #
	#  Copyright (C) Utkarsh Apoorva (utkarsh.apoorva@outlook.com)                                  #
	#                                								#
	#################################################################################################

 
from pymongo import MongoClient
from bson.objectid import ObjectId
import re 

def Connection(_DATABASE):
  from pymongo import MongoClient
  global db 
  db = MongoClient()[_DATABASE]
  print "Connection created with "+_DATABASE+" ..."
  return db


class Collection(object):

  def __init__(self, _db, _name, _schema=None):
    if (_schema): 
      self.schema = _schema
    self.db = _db
    self.collectionName = _name
      
  
  def updateSchema(self, _schema):
    self.schema = _schema

  def keyMatch(self, data, schema):
    global res
    global mismatch  
    res = True
    mismatch = ''
    custom_keys=0
    for k in schema.keys():
      if bool(re.match("^__\w*__$", k)):
        custom_keys += 1
    print custom_keys
    for f in data.keys():
      if ((f not in schema) and (custom_keys<=0)):
        print f+" not found in schema"
        res = False
        mismatch = f
        return res, mismatch
      elif ((f not in schema) and (custom_keys>0)):
        custom_keys -=1
      # For array of objects within the schema. Nested arrays not supported and not recommended in Mongo
      if (isinstance(data[f], list)):
        if (isinstance(schema[f], list)):
          for d in data[f]:
            if type(d) is dict:
              if type(schema[f][0]) is dict:
                self.keyMatch(d, schema[f][0])
              else:
                res = False
                mismatch = d
                return res, mismatch
        else:
          res = False
          return res, mismatch
      if ((type(data[f]) is dict)):
        if (type(schema[f]) is dict):
          self.keyMatch(data[f], schema[f])
        elif (custom_keys>0):
          custom_keys -=1
        else:
          res = False
          mismatch = f
          return res, mismatch
    return res, mismatch
  
   
  def insert(self, data, check=True):
    if (check):
      match, key = self.keyMatch(data, self.schema)
      if not match:
        raise Exception('Fields do not match schema at key: '+key)
    self.db[self.collectionName].insert(data)
    print 'Successfully inserted data'

  def update(self, data):
    match, key = self.keyMatch(data, self.schema)
    if not match:
      raise Exception('Fields do not match schema at key: '+key)
    self.db[self.collectionName].update(data)

  def find(self, data='all'):
    d = []
    if (data=='all'): data = self.db[self.collectionName].find()
    else: data = self.db[self.collectionName].find(data)
    for i in data:
      d.append(i)
    return d

  def findOne(self, data='all'):    
    d = []
    if (data=='all'): data = self.db[self.collectionName].find()
    else: data = self.db[self.collectionName].find(data)
    for i in data:
      d.append(i)
    if len(d)>0:
      return d[0]
    return {}
 
  def remove(self, data):
    self.db[self.collectionName].remove(data)

 












