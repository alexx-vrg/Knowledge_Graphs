// PART 2: THE QUESTIONS



// 1. Find five random user nodes
MATCH (u:User) 
RETURN u 
ORDER BY rand() 
LIMIT 5;

// 2. Find five random FOLLOWS relationships
MATCH p = ()-[:FOLLOWS]->() 
RETURN p 
ORDER BY rand() 
LIMIT 5;

// 3. Find the text property of three random Tweet nodes
MATCH (t:Tweet) 
RETURN t.text 
ORDER BY rand() 
LIMIT 3;

// 4. Generate a Cypher statement to visualize sample RETWEETS relationships
MATCH p = (:Tweet)-[:RETWEETS]->(:Tweet) 
RETURN p 
LIMIT 50;

// 5. Why using merge and not create?
// Answer: CREATE blindly inserts data, potentially creating duplicates. 
// MERGE checks if the pattern exists first (Match or Create), ensuring Idempotency.
RETURN "MERGE prevents duplicates" AS Answer;

// 6. Calculate the ratio of missing values for the createdAt node property
MATCH (t:Tweet)
WITH count(t) AS total, count(t.createdAt) AS present
RETURN 1.0 - (toFloat(present) / total) AS missingRatio;

// 7. Count the number of relationships by their type
MATCH ()-[r]->() 
RETURN type(r) AS relationshipType, count(r) AS count 
ORDER BY count DESC;

// 8. Compare the text of an original tweet and its retweet
MATCH (retweet:Tweet)-[:RETWEETS]->(original:Tweet)
RETURN retweet.text AS RetweetText, original.text AS OriginalText
LIMIT 5;

// 9. Calculate the distribution of tweets grouped by year created
MATCH (t:Tweet)
RETURN t.createdAt.year AS Year, count(t) AS TweetCount
ORDER BY Year;

// 10. Use the MATCH clause in combination with the WHERE clause to select all tweets created in 2021
MATCH (t:Tweet)
WHERE t.createdAt.year = 2021
RETURN t
LIMIT 10;

// 11. Return the top four days with the highest count of created tweets
MATCH (t:Tweet)
RETURN date(t.createdAt) AS Day, count(t) AS TweetCount
ORDER BY TweetCount DESC
LIMIT 4;

// 12. Count the number of users who were mentioned but haven’t published a single tweet
MATCH (u:User)<-[:MENTIONS]-(:Tweet)
WHERE NOT (u)-[:PUBLISH]->(:Tweet)
RETURN count(DISTINCT u) AS PassiveUsers;

// 13. Find the top five users with the most distinct tweets retweeted
MATCH (u:User)-[:PUBLISH]->(t:Tweet)<-[:RETWEETS]-(:Tweet)
RETURN u.username, count(DISTINCT t) AS UniqueTweetsRetweeted
ORDER BY UniqueTweetsRetweeted DESC
LIMIT 5;

// 14. Find the top five most mentioned users
MATCH (u:User)<-[r:MENTIONS]-()
RETURN u.username, count(r) AS MentionCount
ORDER BY MentionCount DESC
LIMIT 5;

// 15. Find the 10 most followed Users
MATCH (u:User)<-[r:FOLLOWS]-()
RETURN u.username, count(r) AS Followers
ORDER BY Followers DESC
LIMIT 10;

// 16. Find the top 10 users who follow the most people
MATCH (u:User)-[r:FOLLOWS]->()
RETURN u.username, count(r) AS Following
ORDER BY Following DESC
LIMIT 10;