from pymongo import MongoClient

# Connect to MongoDB
def connect_db():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["ecommerce"]
    return db

def analyze_campaign_effectiveness(db):
    print("Analyzing campaign effectiveness...")
    
    query = [
        {"$match": {"is_purchased": True}},
        {"$group": {"_id": "$campaign_id", "purchases": {"$sum": 1}}},
        {"$sort": {"purchases": -1}}
    ]
    
    results = list(db.messages.aggregate(query))
    print("Top campaigns that led to purchases:", results)

def recommend_top_products(db):
    print("Finding top personalized recommended products...")
    
    query = [
        {"$match": {"event_type": "purchase"}},
        {"$group": {"_id": "$product_id", "purchase_count": {"$sum": 1}}},
        {"$sort": {"purchase_count": -1}},
        {"$limit": 10}
    ]
    
    results = list(db.events.aggregate(query))
    print("Top 10 recommended products:", results)

def full_text_search_products(db, keyword):
    print(f"Searching for products with keyword: {keyword}...")
    
    query = {"$text": {"$search": keyword}}
    results = list(db.events.find(query, {"product_id": 1, "category_code": 1, "_id": 0}).limit(10))
    
    print("Search results:", results)

def main():
    db = connect_db()
    analyze_campaign_effectiveness(db)
    recommend_top_products(db)
    full_text_search_products(db, "electronics")

if __name__ == "__main__":
    main()
