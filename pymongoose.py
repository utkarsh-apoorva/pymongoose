#!/usr/bin/env python

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
    #custom_keys=0
    custom_keys=[]
    #print schema
    for k in schema.keys():
      if bool(re.match("^__\w*__$", k)):
        #custom_keys += 1
        custom_keys += [k]
    #print custom_keys
    #print schema.keys()
    if (len(custom_keys)>1):
      print "Warning: Bad Structure. More than one custom key found."
      res = False
      mismatch = ''
      return res, mismatch
    for f in data.keys():
      if ((f not in schema) and (len(custom_keys)<=0)):
        print f+" not found in schema"
        res = False
        mismatch = f
        return res, mismatch
      elif ((f not in schema) and (len(custom_keys)>0)):
        #custom_keys = -1
        active_key = custom_keys[0]
        #print 'New Active Key: '+active_key
      if f in schema:
        active_key = f
      # For array of objects within the schema. Nested arrays not supported and not recommended in Mongo
      if (isinstance(data[f], list)):
        if (isinstance(schema[active_key], list)):
          for d in data[f]:
            if type(d) is dict:
              if type(schema[active_key][0]) is dict:
                self.keyMatch(d, schema[active_key][0])
              else:
                res = False
                mismatch = d
                return res, mismatch
        else:
          res = False
          return res, mismatch
      if ((type(data[f]) is dict)):
        if (type(schema[active_key]) is dict):
          self.keyMatch(data[f], schema[active_key])
        #elif len(custom_keys>0):
         # custom_keys -=1
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

  def find(self, conditions={}, projections=None):
    d = []
    data = self.db[self.collectionName].find(conditions,projections)
    for i in data:
      if isinstance(i['_id'], ObjectId):
        i['_id'] = str(i['_id'])
      d.append(i)
    return d

  def findOne(self, data='all'):    
    d = []
    if (data=='all'): data = self.db[self.collectionName].find()
    else: data = self.db[self.collectionName].find(data)
    for i in data:
      if (isinstance(i['_id'], ObjectId)):
        i['_id'] = str(i['_id'])
      d.append(i)
    if len(d)>0:
      return d[0]
    return {}
 
  def remove(self, data):
    self.db[self.collectionName].remove(data)

 












