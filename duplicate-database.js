import { MongoClient, ObjectId } from "mongodb";
import dotenv from "dotenv";

// Load environment variables from .env file
dotenv.config();

// Database duplication script
// This script will copy all data from the current database to a new "workiz-sync" database

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
  console.error("❌ MONGODB_URI is not defined in environment variables");
  console.error(
    "Please check your .env file contains: MONGODB_URI=your-connection-string"
  );
  process.exit(1);
}

async function duplicateDatabase() {
  let sourceClient, targetClient;

  try {
    console.log("🔄 Starting database duplication...");

    // Connect to source database (current database)
    sourceClient = await MongoClient.connect(MONGODB_URI);
    const sourceDb = sourceClient.db();
    console.log(`📊 Connected to source database: ${sourceDb.databaseName}`);

    // Connect to target database (workiz-sync)
    targetClient = await MongoClient.connect(MONGODB_URI);
    const targetDb = targetClient.db("workiz-sync");
    console.log(`📊 Connected to target database: ${targetDb.databaseName}`);

    // Collections to duplicate
    const collections = ["accounts", "jobs", "syncHistory"];

    for (const collectionName of collections) {
      console.log(`\n🔄 Duplicating collection: ${collectionName}`);

      try {
        // Get all documents from source collection
        const documents = await sourceDb
          .collection(collectionName)
          .find({})
          .toArray();
        console.log(
          `📋 Found ${documents.length} documents in ${collectionName}`
        );

        if (documents.length > 0) {
          // Clear target collection first (optional - comment out if you want to append)
          await targetDb.collection(collectionName).deleteMany({});
          console.log(`🗑️  Cleared target collection: ${collectionName}`);

          // Insert documents into target collection
          const result = await targetDb
            .collection(collectionName)
            .insertMany(documents);
          console.log(
            `✅ Successfully copied ${result.insertedCount} documents to ${collectionName}`
          );
        } else {
          console.log(
            `ℹ️  No documents found in ${collectionName}, skipping...`
          );
        }
      } catch (error) {
        console.error(
          `❌ Error duplicating collection ${collectionName}:`,
          error.message
        );
      }
    }

    // Verify the duplication
    console.log("\n🔍 Verifying duplication...");
    for (const collectionName of collections) {
      const sourceCount = await sourceDb
        .collection(collectionName)
        .countDocuments();
      const targetCount = await targetDb
        .collection(collectionName)
        .countDocuments();

      console.log(
        `${collectionName}: ${sourceCount} → ${targetCount} documents`
      );

      if (sourceCount !== targetCount) {
        console.warn(
          `⚠️  Warning: Document count mismatch for ${collectionName}`
        );
      }
    }

    console.log("\n✅ Database duplication completed successfully!");
    console.log(`📊 Source database: ${sourceDb.databaseName}`);
    console.log(`📊 Target database: workiz-sync`);
  } catch (error) {
    console.error("❌ Error during database duplication:", error);
    process.exit(1);
  } finally {
    // Close connections
    if (sourceClient) {
      await sourceClient.close();
      console.log("🔌 Closed source database connection");
    }
    if (targetClient) {
      await targetClient.close();
      console.log("🔌 Closed target database connection");
    }
  }
}

// Run the duplication
duplicateDatabase().catch(console.error);
