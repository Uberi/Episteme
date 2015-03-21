#!/usr/bin/env python3

import sqlite3

class Node:
  def __init__(self, db_connection, subject):
    self.db_connection = db_connection
    self.subject = subject
  
  @property
  def is_known(self):
    records = self.db_connection.execute("SELECT 1 FROM knowledge WHERE subject = ? OR object = ?", (self.subject, self.subject))
    return records.fetchone() != None
  
  @property
  def predicates(self):
    records = self.db_connection.execute("SELECT DISTINCT predicate FROM knowledge WHERE subject = ?", (self.subject,))
    return (row[0] for row in records)
  @property
  def predicates_list(self): return list(self.predicates)
  
  @property
  def referenced_by(self):
    records = self.db_connection.execute("SELECT subject, predicate FROM knowledge WHERE object = ?", (self.subject,))
    return ((Node(self.db_connection, row[0]), row[1]) for row in records)
  @property
  def referenced_by_list(self): return list(self.referenced_by)
  
  def objects(self, predicate):
    if not isinstance(predicate, str): raise TypeError("predicate must be a string")
    records = self.db_connection.execute("SELECT object FROM knowledge WHERE subject = ? AND predicate = ?", (self.subject, predicate))
    return (self.process_value(row[0]) for row in records)
  def __getitem__(self, predicate): return list(self.objects(predicate))
  
  @property
  def info(self):
    records = self.db_connection.execute("SELECT predicate, object FROM knowledge WHERE subject = ?", (self.subject,))
    from collections import defaultdict
    result = defaultdict(list)
    for row in records: result[row[0]].append(self.process_value(row[1]))
    return dict(result)
  
  def process_value(self, value):
    if value.startswith(":"): return value[1:] # check for literal value
    return Node(self.db_connection, value)
  
  def __str__(self): return self.subject
  def __repr__(self): return "<Node subject={}>".format(repr(self.subject))

class Graph:
  def __init__(self, db_path = None):
    if db_path is None:
      from os import path
      db_path = path.join(path.dirname(path.abspath(__file__)), "knowledge.sqlite3")
    self.db_connection = sqlite3.connect(db_path)
  
  def __del__(self):
    self.db_connection.close()
  
  @property
  def subjects(self):
    records = self.db_connection.execute("SELECT DISTINCT subject FROM knowledge")
    return (Node(self.db_connection, row[0]) for row in records)
  
  @property
  def subjects_list(self):
    return list(self.subjects)
  
  def search_subjects(self, pattern):
    import re
    matcher = re.compile(pattern, re.IGNORECASE)
    records = self.db_connection.execute("SELECT DISTINCT subject FROM knowledge")
    return (row[0] for row in records if matcher.match(row[0]))
  def search_subjects_list(self, pattern): return list(self.search_subjects(pattern))
  
  def __getitem__(self, subject):
    if not isinstance(subject, str): raise TypeError("subject must be a string")
    return Node(self.db_connection, subject)
