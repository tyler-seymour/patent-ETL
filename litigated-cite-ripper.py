from sqlalchemy import create_engine
import pandas as pd
import timeit

#sqlalchemy connection object
print("Connecting to patent db...")
engine = create_engine('mysql+pymysql://ipmaster:oroonoko64@ipinformatics.ccmbmjzurfjl.us-west-1.rds.amazonaws.com:3306/ipinformatics')
connection = engine.connect()
print("Complete.")

query1 = "SELECT patent_id, citation_id, date, category, sequence FROM ipinformatics.patentsview_us_patent_citation WHERE patent_id IN("
query2 = "SELECT patent_id, assignee_id FROM ipinformatics.patentsview_patent_assignee WHERE patent_id IN("
query4 = "SELECT patent_id, number AS application_number, date AS grant_date FROM ipinformatics.patentsview_application WHERE patent_id IN("


#read in patent ids
with open("litigated-2hop.txt", "r") as f:
    print("Reading in patent IDs...")
    idlist = f.read().splitlines()
    print("Complete.")

#chunker
chunks = []
subchunk = []

for id in idlist:
    subchunk.append(id)
    if len(subchunk) == 5000:
        yy = ','.join(str(s) for s in subchunk)
        chunks.append(yy)
        subchunk = []
    elif len(chunks) == 4:
        print("FOURTH CHUNK!!")
        if len(subchunk) == 2117:
            yy = ','.join(str(s) for s in subchunk)
            chunks.append(yy)
    print(len(chunks))
    print(len(subchunk))

count = 0

df = pd.DataFrame()

for chizzunk in chunks:
    query_1 = query1 + str(chizzunk) + ")"
    query_2 = query2 + str(chizzunk) + ")"
    query_4 = query4 + str(chizzunk) + ")"

    count = count + 1
    print("Chunk Number " + str(count))

    df1 = pd.read_sql_query(query_1, connection)
    df1.set_index('patent_id')

    df2 = pd.read_sql_query(query_2, connection)
    df2.set_index('patent_id')

    df4 = pd.read_sql_query(query_4, connection)
    df4.set_index('patent_id')

    merge1 = df1.merge(df2, on='patent_id', how='left')
    finalmerge = merge1.merge(df4, on='patent_id', how='left')

    print(finalmerge.head())
    print("Number of records for this query -->")
    print(finalmerge.size)

    df = pd.concat([df, finalmerge])
    print("Total size of dataframe -->")
    print(df.size)

df['assignee_id'] = df.assignee_id.str.replace(r'\r', '')
indexed_df = df.set_index('patent_id')
indexed_df.to_json("litigated-2hop-withIndex.json", orient='index')