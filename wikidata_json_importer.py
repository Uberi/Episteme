#!/usr/bin/env python3

import sqlite3
import json
import sys

LANGUAGE = "en"
DEFAULT_CONFIDENCE = 0.97

def process_term(term):
    term = term.strip()
    if term[0] == "<": # term is an entity
        term = term[1:-1] # remove angle brackets
    elif term[0] == "\"": # term is a literal
        term = term[1:term.rfind("\"")] # remove the quotes, even if the there is stuff after the string
        term = ":" + term.replace("\"\"", "\"").replace("\\\\", "\\") # unescape string
    return term

from urllib.parse import quote
def process_snak(snak):
    if snak["snaktype"] == "value":
        value = snak["datavalue"]["value"]
        if snak["datatype"] == "string": return json.dumps(value)
        if snak["datatype"] == "wikibase-item": return "@Q" + str(value["numeric-id"])
        if snak["datatype"] == "wikibase-property": return "@P" + str(value["numeric-id"])
        if snak["datatype"] == "url": return "#" + value
        if snak["datatype"] == "commonsMedia": return "#https://commons.wikimedia.org/wiki/File:" + quote(value)
        if snak["datatype"] == "monolingualtext": return json.dumps(value["text"])
        return json.dumps((snak["datatype"], snak["datavalue"]["type"], value))
    elif snak["snaktype"] == "somevalue": return True
    elif snak["snaktype"] == "novalue": return None
    raise ValueError("Unknown snaktype: {}".format(snak["snaktype"]))
    
def process_entity(entity):
    entity_id = entity["id"]
    if entity["type"] == "item": is_property = False
    elif entity["type"] == "property": is_property = True
    else: raise ValueError("Unknown entity type: {}".format(entity["type"]))
    yield (entity_id, ":is_property", 1 if is_property else 0)
    
    if "labels" in entity and LANGUAGE in entity["labels"]: yield (entity_id, ":label", json.dumps(entity["labels"][LANGUAGE]["value"]))
    if "descriptions" in entity and LANGUAGE in entity["descriptions"]: yield (entity_id, ":description", json.dumps(entity["descriptions"][LANGUAGE]["value"]))
    if "aliases" in entity and LANGUAGE in entity["aliases"]:
        for alias in entity["aliases"][LANGUAGE]: yield (entity_id, ":alias", json.dumps(alias["value"]))
    
    if "claims" in entity:
        for entity_property, claims_about_property in entity["claims"].items():
            for claim in claims_about_property:
                if claim["rank"] == "deprecated": continue
                main_value = process_snak(claim["mainsnak"])
                yield (entity_id, entity_property, main_value)

def get_entries(tsv_file):
    next(tsv_file) # skip the first line
    for i, line in enumerate(tsv_file):
        line = line.rstrip("\r\n \t,")
        if not line.startswith("{"): continue # skip over the last line and invalid lines
        entity = json.loads(line) # trim the trailing comma
        yield from process_entity(entity)

if len(sys.argv) != 2 and len(sys.argv) != 3:
    print("Usage: {} WIKIDATA_JSON_FILE [SQLITE3_DATABASE_FILE]".format(sys.argv[0]))
    print("    \"WIKIDATA_JSON_FILE\" is a path to a Wikidata-style JSON file (*.json).")
    print("    \"SQLITE3_DATABASE_FILE\" is an path to an Sqlite3 database file (*.sqlite3, *.db). Optional.")
    print("Adds entries from a specified JSON file to a given Sqlite3 database.")
    print("Example: {} wikidata.json".format(sys.argv[0]))
    sys.exit(2)
input_tsv = sys.argv[1]
output_database = sys.argv[2] if len(sys.argv) == 3 else "knowledge.sqlite3"

# set up database for data import
conn = sqlite3.connect(output_database)
conn.execute("PRAGMA synchronous = OFF") # turn off sync to improve speed
conn.execute("PRAGMA cache_size = 10000") # increase cache size from default of 2000 pages
conn.execute("""
    CREATE TABLE IF NOT EXISTS knowledge (
        entity TEXT,
        property TEXT,
        value TEXT,
        PRIMARY KEY (entity, property, value) ON CONFLICT REPLACE)
""")

#import the data from the TSV file
with open(input_tsv, "r") as f:
    conn.executemany("INSERT INTO knowledge VALUES (?, ?, ?)", get_entries(f))

# save all the newly added entries and create indexes to speedup lookup
conn.commit()
conn.execute("CREATE INDEX IF NOT EXISTS entity ON knowledge (entity)")
conn.execute("CREATE INDEX IF NOT EXISTS value ON knowledge (value)")
conn.close()
