#!/usr/bin/env python3

import sqlite3
import sys

DEFAULT_CONFIDENCE = 0.97

def process_term(term):
  term = term.strip()
  if term[0] == "<": # term is an entity
    term = term[1:-1] # remove angle brackets
  elif term[0] == "\"": # term is a literal
    term = term[1:term.rfind("\"")] # remove the quotes, even if the there is stuff after the string
    term = ":" + term.replace("\"\"", "\"").replace("\\\\", "\\") # unescape string
  return term

def get_entries(tsv_file):
  i = 1
  for line in tsv_file:
    fields = line.split("\t", 4)
    if len(fields) > 4 and fields[4].strip() != "": # literal field specified
      literal_value = float(fields[4]) if "." in fields[4] or "e" in fields[4] or "E" in fields[4] else int(fields[4]) # all literal values seem to be numbers
      entry = [process_term(fields[1]), process_term(fields[2]), ":" + str(literal_value), DEFAULT_CONFIDENCE]
    else:
      entry = [process_term(fields[1]), process_term(fields[2]), process_term(fields[3]), DEFAULT_CONFIDENCE]
    yield entry
    i += 1
    if i > 10000: return

if len(sys.argv) != 2 and len(sys.argv) != 3:
  print("Usage: {} YAGO_TSV_FILE [SQLITE3_DATABASE_FILE]".format(sys.argv[0]))
  print("    \"YAGO_TSV_FILE\" is a path to a YAGO-style tab-separated value file (*.tsv).")
  print("    \"SQLITE3_DATABASE_FILE\" is an path to an Sqlite3 database file (*.sqlite3, *.db). Optional.")
  print("Adds entries from a specified TSV file to a given Sqlite3 database.")
  print("Example: {} yagoFacts.tsv".format(sys.argv[0]))
  sys.exit(2)
input_tsv = sys.argv[1]
output_database = sys.argv[2] if len(sys.argv) == 3 else "knowledge.sqlite3"

# set up database for data import
conn = sqlite3.connect(output_database)
conn.execute("PRAGMA synchronous = OFF") # turn off sync to improve speed
conn.execute("PRAGMA cache_size = 10000") # increase cache size from default of 2000 pages
conn.execute("""
  CREATE TABLE IF NOT EXISTS knowledge (
    subject TEXT,
    predicate TEXT,
    object TEXT,
    confidence REAL,
    PRIMARY KEY (subject, predicate, object) ON CONFLICT REPLACE)
""")

#import the data from the TSV file
with open(input_tsv, "r") as f:
  conn.executemany("INSERT INTO knowledge VALUES (?, ?, ?, ?)", get_entries(f))
  
# save all the newly added entries and create indexes to speedup lookup
conn.commit()
conn.execute("CREATE INDEX subject ON knowledge (subject)")
conn.execute("CREATE INDEX object ON knowledge (object)")
conn.close()
