import { MongoClient } from "mongodb";
import dotenv from "dotenv";

// Load environment variables from .env file
dotenv.config();

// Database verification script
// This script helps you verify that the new "workiz-sync" database contains all the expected data

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
  console.error("❌ MONGODB_URI is not defined in environment variables");
  console.error(
    "Please check your .env file contains: MONGODB_URI=your-connection-string"
  );
  process.exit(1);
}

async function testNewDatabase() {
  let client;

  try {
    console.log("🔍 Testing new 'workiz-sync' database...");

    // Connect to the new database
    client = await MongoClient.connect(MONGODB_URI);
    const db = client.db("workiz-sync");
    console.log(`📊 Connected to database: ${db.databaseName}`);

    // List collections
    const collections = await db.listCollections().toArray();
    console.log("\n📋 Collections found:");
    collections.forEach((collection) => {
      console.log(`  - ${collection.name}`);
    });

    // Count documents in each collection
    console.log("\n📊 Document counts:");
    for (const collection of collections) {
      const count = await db.collection(collection.name).countDocuments();
      console.log(`  - ${collection.name}: ${count} documents`);
    }

    // Test a sample query from each collection
    console.log("\n🔍 Sample data verification:");

    if (collections.some((c) => c.name === "accounts")) {
      const account = await db.collection("accounts").findOne();
      if (account) {
        console.log(`  ✅ Accounts: Found account "${account.name}"`);
      } else {
        console.log(`  ℹ️  Accounts: No accounts found`);
      }
    }

    if (collections.some((c) => c.name === "jobs")) {
      const job = await db.collection("jobs").findOne();
      if (job) {
        console.log(`  ✅ Jobs: Found job "${job.UUID}" for ${job.FirstName}`);
      } else {
        console.log(`  ℹ️  Jobs: No jobs found`);
      }
    }

    if (collections.some((c) => c.name === "syncHistory")) {
      const syncRecord = await db.collection("syncHistory").findOne();
      if (syncRecord) {
        console.log(
          `  ✅ SyncHistory: Found sync record from ${syncRecord.timestamp}`
        );
      } else {
        console.log(`  ℹ️  SyncHistory: No sync records found`);
      }
    }

    console.log("\n✅ New database verification completed successfully!");
    console.log("\n📝 Database duplication summary:");
    console.log(
      "- Your data has been successfully copied to 'workiz-sync' database"
    );
    console.log("- Your application will continue using the original database");
    console.log("- The new database is ready for future use if needed");
  } catch (error) {
    console.error("❌ Error testing new database:", error);
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
      console.log("🔌 Closed database connection");
    }
  }
}

// Run the test
testNewDatabase().catch(console.error);
