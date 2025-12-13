// 1. Ensure Customer IDs are unique
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE;

// 2. Ensure Product IDs are unique
CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE;

// 3. Ensure Category IDs are unique
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE;

// 4. Ensure Order IDs are unique
CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE;