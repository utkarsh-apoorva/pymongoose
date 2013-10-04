

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
 

def Connection(_DATABASE):
  print "init Connection"
  global db 
  db = MongoClient()[_DATABASE]
  return db


class Collection(object):

  def __init__(self, _db, _name, _schema):
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
    for f in data.keys():
      if (f not in schema):
        print f+" not found in schema"
        res = False
        mismatch = f
        return res, mismatch
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
        else:
          res = False
          mismatch = f
          return res, mismatch
    return res, mismatch

 
  def insert(self, data):
    match, key = self.keyMatch(data, self.schema)
    if not match:
      raise Exception('Fields do not match schema at key: '+key)
    self.db[self.collectionName].insert(data)

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

 












