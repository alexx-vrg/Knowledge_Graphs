import time
import os
import psycopg2
import pandas as pd
from neo4j import GraphDatabase

# Configuration from Docker Environment
PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "shop")
PG_USER = os.getenv("POSTGRES_USER", "app")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "password")

NEO_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO_USER = os.getenv("NEO4J_USER", "neo4j")
NEO_PASS = os.getenv("NEO4J_PASSWORD", "password")

def wait_for_postgres():
    """Retries connecting to Postgres until it's ready."""
    while True:
        try:
            conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
            conn.close()
            print("Postgres is ready!")
            return
        except psycopg2.OperationalError:
            print("Waiting for Postgres...")
            time.sleep(2)

def wait_for_neo4j():
    """Retries connecting to Neo4j until it's ready."""
    while True:
        try:
            driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS))
            driver.verify_connectivity()
            driver.close()
            print("Neo4j is ready!")
            return
        except Exception:
            print("Waiting for Neo4j...")
            time.sleep(2)

def run_cypher_file(tx, file_path):
    """Reads the queries.cypher file and executes commands."""
    with open(file_path, "r") as f:
        # Split by semicolon to handle multiple commands in one file
        queries = f.read().split(";")
        for query in queries:
            if query.strip():
                tx.run(query)

def etl():
    print("Starting ETL Process...")
    
    # 1. Connection Setup
    wait_for_postgres()
    wait_for_neo4j()
    
    pg_conn = psycopg2.connect(host=PG_HOST, database=PG_DB, user=PG_USER, password=PG_PASS)
    neo_driver = GraphDatabase.driver(NEO_URI, auth=(NEO_USER, NEO_PASS))

    # 2. Initialize Schema (Constraints)
    with neo_driver.session() as session:
        session.execute_write(run_cypher_file, "app/queries.cypher")
        print("Graph Schema applied.")

    # 3. EXTRACT: Read Data from Postgres
    print("Extracting data from SQL...")
    df_categories = pd.read_sql("SELECT * FROM categories", pg_conn)
    df_products = pd.read_sql("SELECT * FROM products", pg_conn)
    df_customers = pd.read_sql("SELECT * FROM customers", pg_conn)
    df_orders = pd.read_sql("SELECT * FROM orders", pg_conn)
    df_items = pd.read_sql("SELECT * FROM order_items", pg_conn)
    df_events = pd.read_sql("SELECT * FROM events", pg_conn)

    # 4. LOAD: Write Data to Neo4j
    print("Loading data into Graph...")
    
    with neo_driver.session() as session:
        # Load Categories
        for _, row in df_categories.iterrows():
            session.run(
                "MERGE (:Category {id: $id, name: $name})",
                parameters={"id": row["id"], "name": row["name"]}
            )
            
        # Load Products and link to Category
        for _, row in df_products.iterrows():
            session.run("""
                MERGE (p:Product {id: $id})
                SET p.name = $name, p.price = $price
                WITH p
                MATCH (c:Category {id: $cat_id})
                MERGE (p)-[:IN_CATEGORY]->(c)
            """, parameters={"id": row["id"], "name": row["name"], 
                             "price": float(row["price"]), "cat_id": row["category_id"]})

        # Load Customers
        for _, row in df_customers.iterrows():
            session.run(
                "MERGE (:Customer {id: $id, name: $name, join_date: toString($date)})",
                parameters={"id": row["id"], "name": row["name"], "date": row["join_date"]}
            )

        # Load Orders and link to Customer
        for _, row in df_orders.iterrows():
            session.run("""
                MERGE (o:Order {id: $id, ts: $ts})
                WITH o
                MATCH (c:Customer {id: $cust_id})
                MERGE (c)-[:PLACED]->(o)
            """, parameters={"id": row["id"], "cust_id": row["customer_id"], "ts": str(row["ts"])})

        # Load Order Items (Link Order to Product)
        for _, row in df_items.iterrows():
            session.run("""
                MATCH (o:Order {id: $oid})
                MATCH (p:Product {id: $pid})
                MERGE (o)-[:CONTAINS {quantity: $qty}]->(p)
            """, parameters={"oid": row["order_id"], "pid": row["product_id"], "qty": row["quantity"]})

        # Load Events (Dynamic Relationships: VIEW, CLICK, ADD_TO_CART)
        for _, row in df_events.iterrows():
            # Dynamically set the relationship type using APOC or string formatting (safe here as values are restricted by SQL CHECK)
            rel_type = row["event_type"].upper() # view -> VIEW
            query = f"""
                MATCH (c:Customer {{id: $cid}})
                MATCH (p:Product {{id: $pid}})
                MERGE (c)-[:{rel_type} {{ts: $ts}}]->(p)
            """
            session.run(query, parameters={"cid": row["customer_id"], 
                                           "pid": row["product_id"], "ts": str(row["ts"])})

    print("ETL Completed Successfully!!")
    pg_conn.close()
    neo_driver.close()

if __name__ == "__main__":
    etl()