// PART 1: DATABASE SETUP & INGESTION


// 1. Resetting the database
MATCH (n) DETACH DELETE n;

// 2. CREATE CONSTRAINTS
CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (t:Tweet) REQUIRE t.id IS UNIQUE;

// 3. LOAD USERS
LOAD CSV WITH HEADERS FROM "https://bit.ly/39JYakC" AS row
MERGE (u:User {id:row.id})
ON CREATE SET u.name = row.name,
              u.username = row.username,
              u.registeredAt = datetime(row.createdAt);

// 4. LOAD TWEETS
// Uses batching to prevent memory timeouts.
LOAD CSV WITH HEADERS FROM "https://bit.ly/3y3ODyc" AS row
CALL {
  WITH row
  MATCH (a:User {id:row.author})
  MERGE (p:Tweet {id:row.id})
  ON CREATE SET p.text = row.text, p.createdAt = datetime(row.createdAt)
  MERGE (a)-[:PUBLISH]->(p)
} IN TRANSACTIONS OF 2000 ROWS;

// 5. LOAD FOLLOWERS
LOAD CSV WITH HEADERS FROM "https://bit.ly/3n08lEL" AS row 
CALL {
    WITH row
    MATCH (s:User {id:row.source})
    MATCH (t:User {id:row.target})
    MERGE (s)-[:FOLLOWS]->(t)
} IN TRANSACTIONS OF 2000 ROWS;

// 6. LOAD INTERACTIONS (Mentions, Retweets, Replies)
// Mentions
LOAD CSV WITH HEADERS FROM "https://bit.ly/3tINZ6D" AS row
MATCH (t:Tweet {id:row.post})
MATCH (u:User {id:row.user})
MERGE (t)-[:MENTIONS]->(u);

// Retweets
LOAD CSV WITH HEADERS FROM "https://bit.ly/3QyDrRl" AS row
MATCH (source:Tweet {id:row.source})
MATCH (target:Tweet {id:row.target})
MERGE (source)-[:RETWEETS]->(target);

// Replies
LOAD CSV WITH HEADERS FROM "https://bit.ly/3b9Wgdx" AS row
MATCH (source:Tweet {id:row.source})
MATCH (target:Tweet {id:row.target})
MERGE (source)-[:IN_REPLY_TO]->(target);

// 7. GDS PROJECTIONS
// 7a. Clean up old projections
CALL gds.graph.drop('twitter', false);
CALL gds.graph.drop('twitter_undirected', false);

// 7b. Create Directed Projection (For PageRank/Influence)
CALL gds.graph.project(
    'twitter',           
    'User',              
    'FOLLOWS'            
);

// 7c. Create Undirected Projection (For PathFinding/Triangles)
CALL gds.graph.project(
    'twitter_undirected',
    'User',
    {
        FOLLOWS: {
            orientation: 'UNDIRECTED'
        }
    }
);

// 8. PRE-CALCULATE METRICS
CALL gds.pageRank.write('twitter', {
    writeProperty: 'pagerank',
    maxIterations: 20,
    dampingFactor: 0.85
});

CALL gds.louvain.write('twitter', {
    writeProperty: 'community'
});