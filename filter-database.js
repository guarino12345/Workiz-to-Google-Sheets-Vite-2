import { MongoClient } from "mongodb";
import dotenv from "dotenv";

// Load environment variables from .env file
dotenv.config();

// Database filtering script
// This script will remove jobs from the database that don't match the sourceFilter configuration

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
  console.error("❌ MONGODB_URI is not defined in environment variables");
  console.error(
    "Please check your .env file contains: MONGODB_URI=your-connection-string"
  );
  process.exit(1);
}

async function filterDatabase() {
  let client;

  try {
    console.log("🔍 Starting database filtering process...");

    // Connect to the database
    client = await MongoClient.connect(MONGODB_URI);
    const db = client.db("workiz-sync");
    console.log(`📊 Connected to database: ${db.databaseName}`);

    // Get all accounts with their source filters
    const accounts = await db.collection("accounts").find({}).toArray();
    console.log(`📋 Found ${accounts.length} accounts`);

    if (accounts.length === 0) {
      console.log("❌ No accounts found in database");
      return;
    }

    let totalJobsBefore = 0;
    let totalJobsAfter = 0;
    let totalJobsRemoved = 0;

    // Process each account
    for (const account of accounts) {
      console.log(`\n🔍 Processing account: ${account.name}`);
      console.log(`📋 Source filter: ${JSON.stringify(account.sourceFilter)}`);

      const accountId = account._id || account.id;

      // Get all jobs for this account
      const allJobs = await db.collection("jobs").find({ accountId }).toArray();
      const jobsBefore = allJobs.length;
      totalJobsBefore += jobsBefore;

      console.log(`📊 Found ${jobsBefore} jobs for this account`);

      if (
        !account.sourceFilter ||
        !Array.isArray(account.sourceFilter) ||
        account.sourceFilter.length === 0
      ) {
        console.log(
          `⚠️ No source filter configured for ${account.name}, skipping...`
        );
        totalJobsAfter += jobsBefore;
        continue;
      }

      // Find jobs that don't match the source filter
      const jobsToRemove = allJobs.filter((job) => {
        // If job has no JobSource, keep it (don't remove)
        if (!job.JobSource) {
          return false;
        }
        // Remove job if its JobSource is not in the sourceFilter
        return !account.sourceFilter.includes(job.JobSource);
      });

      const jobsToKeep = allJobs.filter((job) => {
        // Keep jobs with no JobSource
        if (!job.JobSource) {
          return true;
        }
        // Keep jobs that match the source filter
        return account.sourceFilter.includes(job.JobSource);
      });

      console.log(`🔍 Jobs to keep: ${jobsToKeep.length}`);
      console.log(`🗑️ Jobs to remove: ${jobsToRemove.length}`);

      // Show sample of jobs being removed
      if (jobsToRemove.length > 0) {
        console.log(`📋 Sample jobs being removed:`);
        jobsToRemove.slice(0, 5).forEach((job) => {
          console.log(
            `   - ${job.UUID}: ${job.JobSource} (${job.FirstName} ${job.LastName})`
          );
        });
        if (jobsToRemove.length > 5) {
          console.log(`   ... and ${jobsToRemove.length - 5} more`);
        }
      }

      // Remove jobs that don't match the source filter
      if (jobsToRemove.length > 0) {
        const jobIdsToRemove = jobsToRemove.map((job) => job.UUID);

        const deleteResult = await db.collection("jobs").deleteMany({
          UUID: { $in: jobIdsToRemove },
        });

        console.log(
          `✅ Removed ${deleteResult.deletedCount} jobs from database`
        );
        totalJobsRemoved += deleteResult.deletedCount;
      }

      // Verify final count
      const finalJobs = await db
        .collection("jobs")
        .find({ accountId })
        .toArray();
      const jobsAfter = finalJobs.length;
      totalJobsAfter += jobsAfter;

      console.log(`📈 Final job count for ${account.name}: ${jobsAfter} jobs`);
    }

    // Summary
    console.log(`\n🎯 Database filtering completed!`);
    console.log(`📊 Summary:`);
    console.log(`   - Total jobs before: ${totalJobsBefore}`);
    console.log(`   - Total jobs after: ${totalJobsAfter}`);
    console.log(`   - Total jobs removed: ${totalJobsRemoved}`);
    console.log(
      `   - Database size reduction: ${(
        (totalJobsRemoved / totalJobsBefore) *
        100
      ).toFixed(1)}%`
    );

    // Show final collection stats
    const finalTotalJobs = await db.collection("jobs").countDocuments();
    console.log(`\n📊 Final database stats:`);
    console.log(`   - Total jobs in database: ${finalTotalJobs}`);

    const collections = await db.listCollections().toArray();
    console.log(
      `   - Collections: ${collections.map((c) => c.name).join(", ")}`
    );

    console.log(`\n✅ Database filtering process completed successfully!`);
  } catch (error) {
    console.error("❌ Error during database filtering:", error);
    process.exit(1);
  } finally {
    if (client) {
      await client.close();
      console.log("🔌 Closed database connection");
    }
  }
}

// Run the filtering
filterDatabase().catch(console.error);
