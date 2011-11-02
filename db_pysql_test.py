#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
import unittest
import os
import itertools
import random
from pysql import *
class DBTests(unittest.TestCase):
    def test_create_table(self):
        if os.path.exists("example.db"):
            os.remove("example.db")
        db = DB("example.db")
        db.create('Products', ('name', 'price'))
        rows = db.find({'from':'pysql_master',})
        self.assertEqual(("Products", 2, "name,price"), next(rows))
        self.assertRaises(StopIteration, next, rows)
    def test_insert(self):
        if os.path.exists("example2.db"):
            os.remove("example2.db")
        db = DB("example2.db")
        db.create('Products', ('name', 'price'))
        rows = db.find({'from':'Products',})
        self.assertRaises(StopIteration, next, rows)
        inp= [("りんご", 30),
              ("みかん", 50)]
        for v in inp:
            db.insert("Products", v)
        rows = db.find({'from':'Products',})
        for v in inp:
            self.assertEqual(v, next(rows))
        self.assertRaises(StopIteration, next, rows)
    def test_find_cols(self):
        db = DB("test.db")
        rows = db.find({'from':'pysql_master', 'cols':('rootpage', 'tab_name')})
        expections = [(2, 'Products'),
                      (3, 'factorials')
                     ]
        for expection in expections:
            self.assertEqual(expection, next(rows))
        self.assertRaises(StopIteration, next, rows)        
        rows = db.find({'from':'pysql_master', 'cols':('rootpage',)})        
        for expection in expections:
            self.assertEqual(expection[0], next(rows)[0])
        self.assertRaises(StopIteration, next, rows)
        rows = db.find({'from':'pysql_master', 'cols':('tab_name',)})        
        for expection in expections:
            self.assertEqual(expection[1], next(rows)[0])
        self.assertRaises(StopIteration, next, rows)
    @staticmethod
    def make_record():
        g = itertools.permutations("abcdefghij")
        db = DB("test2.db")
        db.create("random", ("id", "text"))
        values = []
        for idx  in range(20):
            values.append((idx, ''.join(next(g))*10))
        random.shuffle(values)
        for v in values:
            db.insert("random", v)
    def test_orderby(self):
        if os.path.exists("example2.db"):
            os.remove("example2.db")
        DBTests.make_record()            
        db = DB("test2.db")
        db.create('Products', ('name', 'price'))
        rows = db.find({'from':'random', 'orderby':('id',)})
        g = itertools.permutations("abcdefghij")
        for idx  in range(20):
            v = (idx, ''.join(next(g))*10)
            self.assertEqual(v, next(rows))
        self.assertRaises(StopIteration, next, rows) 
if __name__ == '__main__':
    unittest.main()
