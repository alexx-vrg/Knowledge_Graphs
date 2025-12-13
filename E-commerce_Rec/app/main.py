from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
import os

app = FastAPI()

# Database Connection
URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))

def get_db_driver():
    return GraphDatabase.driver(URI, auth=AUTH)

@app.get("/")
def read_root():
    return {"message": "Welcome to the E-commerce Graph Lab"}

@app.get("/health")
def health_check():
    return {"ok": True}

@app.get("/recommendations/{customer_id}")
def get_recommendations(customer_id: str):
    """
    Collaborative Filtering:
    Recommend products purchased by others who bought similar items.
    """
    cypher_query = """
    // 1. Find products the target customer purchased
    MATCH (c:Customer {id: $customer_id})-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
    
    // 2. Find OTHER customers who bought those same products
    MATCH (p)<-[:CONTAINS]-(:Order)<-[:PLACED]-(other:Customer)
    WHERE other <> c
    
    // 3. Find other products those customers bought
    MATCH (other)-[:PLACED]->(:Order)-[:CONTAINS]->(rec:Product)
    WHERE NOT (c)-[:PLACED]->(:Order)-[:CONTAINS]->(rec)
    
    // 4. Return the most popular recommendations
    RETURN rec.name AS product, rec.price AS price, count(*) AS score
    ORDER BY score DESC
    LIMIT 5
    """
    
    driver = get_db_driver()
    try:
        with driver.session() as session:
            result = session.run(cypher_query, customer_id=customer_id)
            recommendations = [record.data() for record in result]
            
        if not recommendations:
            return {"customer_id": customer_id, "message": "No recommendations found (or new user)."}
            
        return {"customer_id": customer_id, "recommendations": recommendations}
    finally:
        driver.close()