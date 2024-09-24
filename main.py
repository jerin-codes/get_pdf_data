
import tabula
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# Extract tables from PDF
tables = tabula.read_pdf("thrissur vs kannur.pdf", pages="all", multiple_tables=True)

print(f"Number of tables extracted: {len(tables)}")

structured_data = []

# Iterate over tables to extract data
for i, table in enumerate(tables):
    if (i + 1) == 3 or (i + 1) == 5:
        print(f"\nExtracting data from Table {i + 1}:")
       
        table = table[['No', 'Pos', 'Name']].dropna()

        for index, row in table.iterrows():
            player_data = {
                "jersy_no": int(row["No"]),           
                "pos": row["Pos"],             
                "name": row["Name"].split('(')[0].strip(),  
                "team": "Kannur Warriors FC"  # Change this for other teams like Kochi, Trivandrum, etc.
            }
            structured_data.append(player_data)

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/") 
db = client["player_database"]
collection = db["Kannur"]  # Change this for other collections

# Insert data with error handling for duplicate entries
for player in structured_data:
    try:
        result = collection.insert_one(player)
        print(f"Inserted: {player}")
    except DuplicateKeyError:
        print(f"Duplicate entry for jersey number: {player['jersy_no']}, skipping insertion.")

# Close MongoDB connection
client.close()
