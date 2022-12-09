import pandas as pd
import json
import os
import pymongo

#loading bar to see how long it will take to insert data
from tqdm import tqdm

#clearing screen
os.system("cls" if os.name == "nt" else "clear")

#constants
URI = "mongodb://admin:Sp00ky!@localhost:27017/?AuthSource=admin"
DIAMOND_CSV = "DiamondsPrices.csv"
DB_NAME = "diamond_prices"
COLLECTION_NAME = "diamonds"

#connecting to mongoDB and creating a datafram out of the CSV
client = pymongo.MongoClient(URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
diamonds_df = pd.read_csv(DIAMOND_CSV).drop_duplicates()
df_length = len(diamonds_df)

#drop the collection if it already exists
if COLLECTION_NAME in db.list_collection_names():
    print(f"\nThe collection '{COLLECTION_NAME}' is being deleted as it already exists...")
    collection.drop()
    
#validation 
collection = db.create_collection(
    name = COLLECTION_NAME,
    validator={"$jsonSchema": {
        "required": ["diamond_id", "carat", "price", "appearance", "size", "dimensions"],
        "properties":
        {
            "diamond_id": {
                "bsonType": "int",
                "description": "Unique ID for the diamond"
            },
            
            "carat": {
                "description": "Unit of weight for stones and pearls"
            },
            
            "price": {
                "bsonType": "int",
                "description": "USD price"
            },
            
            "appearance": {
                "bsonType": ["array"],

                "items": {
                    "required": ["cut", "colour", "clarity"],
                    
                    "properties":{
                        "cut": {
                            "bsonType": "string",
                            "description": "Stone shape score",
                            "enum": ["Ideal", "Premium", "Very Good", "Good", "Fair"]
                        },
                        
                        "colour": {
                            "bsonType": "string",
                            "description": "Diamond Color Scale",
                            "enum": ["D","E","F","G","H","I","J"]
                        },
                        
                        "clarity": {
                            "bsonType": "string",
                            "description": "Diamond Clarity Scale",
                            "enum": ["IF","VVS1","VVS2","VS1","VS2","SI1","SI2","I1"]
                        }
                    }
                }  
            },
                       
            "size": {
                "bsonType": ["array"],
                "items": {
                    "required": ["depth", "table"],
                    
                    "properties":{
                        "depth": {
                            "description": "percentage format of z/x"
                        },
                        
                        "table": {
                            "description": "percentage format of z/a"
                        }
                    }
                }
            },
            
            "dimensions": {
                "bsonType": ["array"],
                "items": {
                    "required": ["x", "y", "z"],
                    "additionalProperties": False,

                    "properties":{
                        "x": {
                            "description": "x dimensions"
                        },
                        
                        "y": {
                            "description": "y dimensions"
                        },
                        
                        "z": {
                            "description": "z dimensions"
                        }
                    }
                }
            }
        }
    }}, validationAction="error",)


print(f"\nPlease be patient, adding {df_length} documents to the {COLLECTION_NAME} collection...\n")

#iterating through each row, converting to them to json and inserting to collection
for diamond_id, row in tqdm(enumerate(diamonds_df.itertuples()), total=df_length):

    diamond_document = json.dumps({
        "diamond_id": diamond_id,
        "carat": row.carat,
        "price": row.price,
        "appearance":[
            {
                "cut": row.cut,
                "colour": row.color,
                "clarity": row.clarity
            }
        ],

        "size":[
            {
                "depth": row.depth,
                "table": row.table
            }
        ],
        
        "dimensions": [
            {
                "x": row.x,
                "y": row.y,
                "z": row.y
            }
        ]
    })
     
    #inserting diamond_document into collection
    collection.insert_one(json.loads(diamond_document))

#to show documents have actually been inserted using this code
print(f"\nLast inserted document:\n\n{json.dumps(json.loads(diamond_document), indent=2)}")

#letting user know all collections have been added
print("\nAll complete!\n")