import rdflib

# but this is
f0 = open("example.nt").read()

graph = rdflib.Graph()

graph.parse(data=f0, format="n3")

predicate_query = graph.query("""
                     select ?company ?product
                     where {
                     ?site <http://graph.ir.ee/media/usedBy> ?company .
                     ?site <http://www.w3.org/1999/xhtml/microdata#item> ?product .
                     ?product <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Product> .
                     }	
                     """)

for row in predicate_query:
    print(row)