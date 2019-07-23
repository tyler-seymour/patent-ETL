from sqlalchemy import create_engine
import math as m
import pandas as pd

#sqlalchemy connection object
print("Connecting to patent db...")
engine = create_engine('mysql+pymysql://ipmaster:oroonoko64@ipinformatics.ccmbmjzurfjl.us-west-1.rds.amazonaws.com:3306/ipinformatics')
connection = engine.connect()
print("Complete.")

#read in patent ids
with open("patent_ids_705.csv", "r") as f:
    print("Reading in patent IDs...")
    idlist = f.read().splitlines()
    print("Complete.")
    print(idlist)
    print(len(idlist))

#chunker
chunks = []
sqlChunks = []
chunk = []
chunkSize = 5000
maxchunks = int(m.ceil(len(idlist)/(float(chunkSize)))) #+ (len(idlist)%5)>0?1:0
print(maxchunks)

print("Total chunks to do: " + str(maxchunks))

count = 0


def sqlStr(chnk):
    s = ','.join(str(s) for s in chnk)
    return s[1:len(s) - 1]


for id in idlist:
    if len(chunk) < chunkSize:
        chunk.append(id)
    else:
        chunks.append(chunk)
        sqlChunks.append(sqlStr(chunk))
        chunk = [id]

if len(chunk) > 0:
    chunks.append(chunk)
    sqlChunks.append(sqlStr(chunk))

print("Chunks: ")
print(len(chunks))
print(chunks)

# Lookup Query Redux

lookup = "SELECT s_no as application_id, filing_date, patent_no, date as grant_date, organization as company FROM iplogic.pair_application INNER JOIN ipinformatics.patentsview_patent ON iplogic.pair_application.patent_no = ipinformatics.patentsview_patent.id INNER JOIN ipinformatics.patentsview_raw_assignee ON iplogic.pair_application.patent_no = ipinformatics.patentsview_raw_assignee.patent_id WHERE iplogic.pair_application.patent_no IN ("

count1 = 0
#df1 = pd.DataFrame(index =['application_id'], columns = ['application_id', 'filing_date', 'patent_no', 'grant_date', 'company'])

initial_frame = None
for i in range(len(chunks)):
    print("Chunk no. " + str(i))
    query = lookup + str(sqlChunks[i]) + ")"
    print(query)
    frame = pd.read_sql_query(query, connection)

    if initial_frame is None:
        initial_frame = frame
    else:
        initial_frame = pd.concat([initial_frame, frame], axis=0)
        initial_frame.reset_index(drop=True, inplace=True)

    print(initial_frame.head(5))
    print(initial_frame.shape)

initial_frame.to_json("ultrarip666.json", orient='records', lines=True)