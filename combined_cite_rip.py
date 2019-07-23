from sqlalchemy import create_engine
import pandas as pd
import timeit

#sqlalchemy connection object
print("Connecting to patent db...")
engine = create_engine('mysql+pymysql://ipmaster:oroonoko64@ipinformatics.ccmbmjzurfjl.us-west-1.rds.amazonaws.com:3306/ipinformatics')
connection = engine.connect()
print("Complete.")

query1 = "SELECT * FROM iplogic.patent_citation WHERE application_id IN(" + str(id)

#read in patent ids
with open("705_grant_application_ids1.txt", "r") as f:
    print("Reading in application IDs...")
    idlist = f.read().splitlines()
    print("Complete.")

#with open("705_output_combined.csv"):
#    for id_chunk in range()
#        query1 = "SELECT * FROM iplogic.patent_citation WHERE application_id IN(" + id

#chunker
chunks = []
subchunk = []
chunkcount = 0

for id in idlist:
    subchunk.append(id)
    if len(subchunk) == 999:
        yy = ','.join(str(s) for s in subchunk)
        chunks.append(yy)
        subchunk = []

    elif len(idlist) - ((len(chunks)-1)*1000) > 999:
        subchunk.append(id)
        yy = ','.join(str(s) for s in subchunk)
        chunks.append(yy)
    print(len(chunks))
    print(len(subchunk))

