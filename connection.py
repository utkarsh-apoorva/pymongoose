

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
        #                                                                                               #
        #################################################################################################


# import Database Settings

#import MongoDB 
#db=MongoDB.db

print "conn mod from usr/local/lib"

class Connection:
  def __init__(self, _DATABASE):
    print "init Connection"
    from pymongo import MongoClient
    self.db = MongoClient()[_DATABASE]

