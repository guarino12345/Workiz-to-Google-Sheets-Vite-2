const { MongoClient, ObjectId } = require("mongodb");

async function addDatabaseIndexes() {
  const uri = process.env.MONGODB_URI;
  if (!uri) {
    console.error("MONGODB_URI environment variable is required");
    process.exit(1);
  }

  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB");

    const db = client.db();

    // Indexes for jobs collection
    console.log("Creating indexes for jobs collection...");
    await db
      .collection("jobs")
      .createIndex({ UUID: 1, accountId: 1 }, { unique: true });
    await db.collection("jobs").createIndex({ accountId: 1 });
    await db.collection("jobs").createIndex({ lastUpdated: 1 });
    await db.collection("jobs").createIndex({ JobDateTime: 1 });

    // Indexes for batches collection
    console.log("Creating indexes for batches collection...");
    await db.collection("batches").createIndex({ operationId: 1 });
    await db.collection("batches").createIndex({ accountId: 1 });
    await db.collection("batches").createIndex({ status: 1 });
    await db
      .collection("batches")
      .createIndex({ operationId: 1, accountId: 1, status: 1 });

    // Indexes for batchAccountStates collection
    console.log("Creating indexes for batchAccountStates collection...");
    await db.collection("batchAccountStates").createIndex({ operationId: 1 });
    await db.collection("batchAccountStates").createIndex({ accountId: 1 });
    await db
      .collection("batchAccountStates")
      .createIndex({ operationId: 1, accountId: 1 });

    // Indexes for accounts collection
    console.log("Creating indexes for accounts collection...");
    await db.collection("accounts").createIndex({ workizApiKey: 1 });

    console.log("All indexes created successfully!");
  } catch (error) {
    console.error("Error creating indexes:", error);
  } finally {
    await client.close();
  }
}

addDatabaseIndexes();
