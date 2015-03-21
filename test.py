#!/usr/bin/env python3

import episteme

g = episteme.Graph()
print("arbitrary subject:", next(g.subjects))
print("search results for \"red.*apple\":", g.search_subjects_list("red.*apple"))
node = g["Sad_Movie"]
print(node, "is known:", node.is_known)
print(node, "predicates:", node.predicates_list)
print(node, "is referenced by:", node.referenced_by_list)
print(node, "has duration:", node["hasDuration"])
print(node, "was created on:", node["wasCreatedOnDate"])
