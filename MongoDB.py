#!/usr/bin/python
#filename: MongoDB.py


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
 



_DATABASE = 'test_db'




from pymongo import MongoClient
db = MongoClient()[_DATABASE]
schema = db.schema




