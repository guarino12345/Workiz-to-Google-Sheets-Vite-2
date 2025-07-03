import express from "express";
import cors from "cors";
import { MongoClient, ObjectId } from "mongodb";
import dotenv from "dotenv";
import { google } from "googleapis";
import { formatInTimeZone } from "date-fns-tz";

dotenv.config();

const app = express();
const port = process.env.PORT || 3001;

// Enhanced error handling utilities

// Circuit Breaker Pattern for API resilience
class CircuitBreaker {
  constructor(failureThreshold = 5, recoveryTimeout = 300000) {
    // 5 failures, 5 minutes recovery
    this.failureThreshold = failureThreshold;
    this.recoveryTimeout = recoveryTimeout;
    this.failureCount = 0;
    this.lastFailureTime = null;
    this.state = "CLOSED"; // CLOSED, OPEN, HALF_OPEN
  }

  async execute(operation) {
    if (this.state === "OPEN") {
      if (Date.now() - this.lastFailureTime >= this.recoveryTimeout) {
        console.log("🔄 Circuit breaker transitioning to HALF_OPEN state");
        this.state = "HALF_OPEN";
      } else {
        throw new Error("Circuit breaker is OPEN - too many recent failures");
      }
    }

    try {
      const result = await operation();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  onSuccess() {
    this.failureCount = 0;
    this.state = "CLOSED";
    console.log("✅ Circuit breaker reset to CLOSED state");
  }

  onFailure() {
    this.failureCount++;
    this.lastFailureTime = Date.now();

    if (this.failureCount >= this.failureThreshold) {
      this.state = "OPEN";
      console.log(
        `🚨 Circuit breaker opened after ${
          this.failureCount
        } failures. Will retry in ${this.recoveryTimeout / 1000} seconds`
      );
    } else {
      console.log(
        `⚠️ Circuit breaker failure count: ${this.failureCount}/${this.failureThreshold}`
      );
    }
  }

  getState() {
    return {
      state: this.state,
      failureCount: this.failureCount,
      lastFailureTime: this.lastFailureTime,
      timeUntilRecovery:
        this.state === "OPEN"
          ? Math.max(
              0,
              this.recoveryTimeout - (Date.now() - this.lastFailureTime)
            )
          : 0,
    };
  }
}

// Global circuit breaker instances
const workizCircuitBreaker = new CircuitBreaker(5, 300000); // 5 failures, 5 minutes recovery
const sheetsCircuitBreaker = new CircuitBreaker(3, 180000); // 3 failures, 3 minutes recovery

// Enhanced error handling with retry logic
class RetryHandler {
  static async withRetry(
    operation,
    maxRetries = 3,
    delay = 1000,
    circuitBreaker = null
  ) {
    let lastError;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        // Use circuit breaker if provided
        if (circuitBreaker) {
          return await circuitBreaker.execute(operation);
        }
        return await operation();
      } catch (error) {
        lastError = error;

        // Check if it's a circuit breaker error
        if (error.message.includes("Circuit breaker is OPEN")) {
          console.log(`🚨 Circuit breaker blocked operation: ${error.message}`);
          throw error;
        }

        // Handle 520 errors with longer delays
        const is520Error =
          error.message.includes("520") ||
          (error.response && error.response.status === 520);

        if (is520Error) {
          console.log(
            `⚠️ 520 error detected on attempt ${attempt}, using extended delay`
          );
          // Use longer delays for 520 errors: 10s, 20s, 40s
          const waitTime = 10000 * Math.pow(2, attempt - 1);
          console.log(
            `⏳ Waiting ${waitTime / 1000}s before retry due to 520 error`
          );
          await new Promise((resolve) => setTimeout(resolve, waitTime));
        } else if (attempt === maxRetries) {
          throw error;
        } else {
          // Exponential backoff for other errors
          const waitTime = delay * Math.pow(2, attempt - 1);
          console.log(
            `⚠️ Attempt ${attempt} failed, retrying in ${waitTime}ms: ${error.message}`
          );
          await new Promise((resolve) => setTimeout(resolve, waitTime));
        }
      }
    }
  }
}

// API rate limiting and timeout handling
class APIManager {
  static async fetchWithTimeout(url, options = {}, timeout = 30000) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), timeout);

    try {
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
      });
      clearTimeout(timeoutId);

      // Check for 520 error specifically
      if (response.status === 520) {
        console.log(`🚨 520 error detected from Workiz API`);
        throw new Error(`Workiz API 520 error - server is experiencing issues`);
      }

      return response;
    } catch (error) {
      clearTimeout(timeoutId);
      if (error.name === "AbortError") {
        throw new Error(`Request timeout after ${timeout}ms`);
      }
      throw error;
    }
  }

  static async handleRateLimit(response, retryAfter = 60) {
    if (response.status === 429) {
      console.log(`⏳ Rate limited, waiting ${retryAfter} seconds...`);
      await new Promise((resolve) => setTimeout(resolve, retryAfter * 1000));
      return true;
    }
    return false;
  }
}

// Database connection management with health checks
class DatabaseManager {
  static async healthCheck(db) {
    try {
      await db.admin().ping();
      return true;
    } catch (error) {
      console.error("❌ Database health check failed:", error.message);
      return false;
    }
  }

  static async ensureHealthyConnection(db) {
    const isHealthy = await this.healthCheck(db);
    if (!isHealthy) {
      throw new Error("Database connection is unhealthy");
    }
  }
}

// Helper function to calculate start date (60 days ago)
function getStartDate() {
  const sixtyDaysAgo = new Date();
  sixtyDaysAgo.setDate(sixtyDaysAgo.getDate() - 60);
  return sixtyDaysAgo.toISOString().split("T")[0]; // Format as YYYY-MM-DD
}

// Global error handler
process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
});

process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  process.exit(1);
});

app.use(cors());
app.use(express.json());

// Global error handler middleware
app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);
  res.status(500).json({
    error: "Internal server error",
    message: err.message,
    timestamp: new Date().toISOString(),
  });
});

// Health check endpoint
app.get("/api/health", (req, res) => {
  res.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    message: "Server is running",
  });
});

// Database test endpoint
app.get("/api/test-db", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const collections = await db.listCollections().toArray();
    res.json({
      status: "ok",
      message: "Database connection successful",
      collections: collections.map((c) => c.name),
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error("Database test failed:", error);
    res.status(500).json({
      status: "error",
      message: "Database connection failed",
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

const MONGODB_URI = process.env.MONGODB_URI;

if (!MONGODB_URI) {
  console.error("MONGODB_URI is not defined in environment variables");
  process.exit(1);
}

let db;

async function connectToMongoDB() {
  try {
    const client = await MongoClient.connect(MONGODB_URI);
    db = client.db("workiz-sync");
    console.log("Connected to MongoDB Atlas");
  } catch (error) {
    console.error("Error connecting to MongoDB:", error);
    process.exit(1);
  }
}

// Helper function to ensure database connection
async function ensureDbConnection() {
  if (!db) {
    console.log("🔄 Establishing database connection...");
    await connectToMongoDB();
  }
  return db;
}

// Account management endpoints
app.post("/api/accounts", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const accountData = {
      ...req.body,
      syncEnabled: false, // Disabled by default - using Vercel cron jobs instead
      syncFrequency: req.body.syncFrequency ?? "daily",
      syncTime: req.body.syncTime ?? "09:00",
      createdAt: new Date(),
      updatedAt: new Date(),
    };
    const result = await db.collection("accounts").insertOne(accountData);
    res.json(result);
  } catch (error) {
    console.error("Error creating account:", error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/accounts", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const accounts = await db.collection("accounts").find().toArray();
    // Transform _id to id for frontend
    const transformedAccounts = accounts.map((account) => ({
      ...account,
      id: account._id.toString(),
      _id: undefined,
    }));
    res.json(transformedAccounts);
  } catch (error) {
    console.error("Error fetching accounts:", error);
    res.status(500).json({ error: error.message });
  }
});

app.put("/api/accounts/:id", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const { id, ...updateData } = req.body;
    const updatePayload = {
      ...updateData,
      updatedAt: new Date(),
    };

    const result = await db
      .collection("accounts")
      .updateOne({ _id: new ObjectId(req.params.id) }, { $set: updatePayload });

    if (result.matchedCount === 0) {
      return res.status(404).json({ error: "Account not found" });
    }

    const updatedAccount = await db
      .collection("accounts")
      .findOne({ _id: new ObjectId(req.params.id) });

    // Transform _id to id for frontend
    const transformedAccount = {
      ...updatedAccount,
      id: updatedAccount._id.toString(),
      _id: undefined,
    };

    res.json(transformedAccount);
  } catch (error) {
    console.error("Error updating account:", error);
    res.status(500).json({ error: error.message });
  }
});

app.delete("/api/accounts/:id", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const result = await db.collection("accounts").deleteOne({
      _id: new ObjectId(req.params.id),
    });

    if (result.deletedCount === 0) {
      return res.status(404).json({ error: "Account not found" });
    }

    res.json({ message: "Account deleted successfully" });
  } catch (error) {
    console.error("Error deleting account:", error);
    res.status(500).json({ error: error.message });
  }
});

// Sync history endpoints
app.post("/api/sync-history", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const syncHistoryData = {
      ...req.body,
      timestamp: new Date(),
      createdAt: new Date(),
    };
    const result = await db
      .collection("syncHistory")
      .insertOne(syncHistoryData);
    res.json(result);
  } catch (error) {
    console.error("Error creating sync history:", error);
    res.status(500).json({ error: error.message });
  }
});

app.get("/api/sync-history/:accountId", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const { accountId } = req.params;

    // Try to find sync history with both ObjectId and string formats
    const syncHistory = await db
      .collection("syncHistory")
      .find({
        $or: [{ accountId: accountId }, { accountId: new ObjectId(accountId) }],
      })
      .sort({ timestamp: -1 })
      .limit(50) // Limit to last 50 sync records
      .toArray();

    // Transform _id to id for frontend
    const transformedHistory = syncHistory.map((record) => ({
      ...record,
      id: record._id.toString(),
      _id: undefined,
    }));

    res.json(transformedHistory);
  } catch (error) {
    console.error("Error fetching sync history:", error);
    res.status(500).json({ error: error.message });
  }
});

// Jobs endpoints
app.get("/api/jobs", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const jobs = await db
      .collection("jobs")
      .find()
      .sort({ JobDateTime: -1 })
      .toArray();
    res.json(jobs);
  } catch (error) {
    console.error("Error fetching jobs:", error);
    res.status(500).json({ error: error.message });
  }
});

// Sync jobs from Workiz and save to MongoDB
app.post("/api/sync-jobs/:accountId", async (req, res) => {
  const accountStartTime = Date.now();

  try {
    const db = await ensureDbConnection();
    const { accountId } = req.params;
    console.log(`🔍 Starting sync for account ID: ${accountId}`);

    // Find account by ID - try both id and _id fields
    const account = await db.collection("accounts").findOne({
      $or: [{ _id: new ObjectId(accountId) }, { id: accountId }],
    });

    if (!account) {
      console.log(`❌ Account not found for ID: ${accountId}`);
      return res.status(404).json({ error: "Account not found" });
    }

    console.log(
      `✅ Found account: ${account.name} (API Token: ${
        account.workizApiToken ? "Present" : "Missing"
      })`
    );

    if (!account.workizApiToken) {
      return res
        .status(400)
        .json({ error: "Missing API token for this account" });
    }

    // Fetch jobs from Workiz API using the token from the account
    const startDate = getStartDate();
    const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/all/?start_date=${startDate}&offset=0&records=100&only_open=false`;
    console.log(`🌐 Fetching from Workiz: ${workizUrl}`);

    const response = await RetryHandler.withRetry(
      async () => {
        const resp = await APIManager.fetchWithTimeout(workizUrl, {}, 45000);

        if (!resp.ok) {
          const errorText = await resp.text();
          console.log(`❌ Workiz API error: ${resp.status} - ${errorText}`);

          // Check if response is HTML (520 error page)
          if (
            errorText.includes('<div class="text-container">') ||
            errorText.includes("Oops!") ||
            errorText.includes("Something went wrong")
          ) {
            console.log(
              `🚨 Detected HTML error page from Workiz API (likely 520 error)`
            );
            throw new Error(
              `Workiz API 520 error - server is experiencing issues`
            );
          }

          throw new Error(`Workiz API error: ${resp.status} - ${errorText}`);
        }

        return resp;
      },
      5,
      2000,
      workizCircuitBreaker
    ); // 5 retries, 2s base delay, with circuit breaker

    const data = await response.json();
    console.log(
      `📊 Workiz API response: flag=${data.flag}, data.length=${
        data.data?.length || 0
      }`
    );

    if (!data.flag || !Array.isArray(data.data)) {
      console.log(`❌ Invalid Workiz API response structure`);
      return res
        .status(500)
        .json({ error: "Invalid response from Workiz API" });
    }

    console.log(`📋 Processing ${data.data.length} jobs from Workiz`);

    // Add accountId to each job
    const jobs = data.data.map((job) => ({
      ...job,
      accountId: account._id || account.id,
    }));

    console.log(
      `🏷️ Added accountId to jobs. Sample job UUIDs: ${jobs
        .slice(0, 3)
        .map((j) => j.UUID)
        .join(", ")}`
    );

    // Check for existing jobs with same UUIDs
    const existingJobCount = await db.collection("jobs").countDocuments({
      UUID: { $in: jobs.map((job) => job.UUID) },
    });
    console.log(
      `🔍 Found ${existingJobCount} existing jobs with matching UUIDs`
    );

    // Upsert jobs into MongoDB
    const bulkOps = jobs.map((job) => ({
      updateOne: {
        filter: { UUID: job.UUID },
        update: { $set: job },
        upsert: true,
      },
    }));

    console.log(`💾 Executing bulk write with ${bulkOps.length} operations`);

    if (bulkOps.length > 0) {
      const bulkResult = await db.collection("jobs").bulkWrite(bulkOps);
      console.log(`✅ Bulk write completed:`, {
        matchedCount: bulkResult.matchedCount,
        modifiedCount: bulkResult.modifiedCount,
        upsertedCount: bulkResult.upsertedCount,
        insertedCount: bulkResult.insertedCount,
      });
    }

    // Verify final count
    const finalJobCount = await db.collection("jobs").countDocuments({
      accountId: account._id || account.id,
    });
    console.log(`📈 Final job count for account: ${finalJobCount}`);

    // Update existing jobs and clean up old ones
    console.log(`🔄 Starting job update and cleanup process...`);

    // Get all existing jobs for this account
    const existingJobs = await db
      .collection("jobs")
      .find({ accountId: account._id || account.id })
      .toArray();

    console.log(`📋 Found ${existingJobs.length} existing jobs in database`);

    // Calculate 1-year cutoff date
    const oneYearAgo = new Date();
    oneYearAgo.setDate(oneYearAgo.getDate() - 365);

    let updatedJobsCount = 0;
    let deletedJobsCount = 0;
    let failedUpdatesCount = 0;

    // Process jobs in batches to avoid memory issues and rate limiting
    const BATCH_SIZE = 29;
    const DELAY_BETWEEN_BATCHES = 60000; // 60 seconds in milliseconds

    for (let i = 0; i < existingJobs.length; i += BATCH_SIZE) {
      const batch = existingJobs.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(existingJobs.length / BATCH_SIZE);

      console.log(
        `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} jobs)`
      );

      // Process each job in the current batch
      for (const existingJob of batch) {
        try {
          const jobDate = new Date(existingJob.JobDateTime);

          // Check if job is older than 1 year
          if (jobDate < oneYearAgo) {
            console.log(
              `🗑️ Deleting old job: ${existingJob.UUID} (${existingJob.JobDateTime})`
            );
            await RetryHandler.withRetry(async () => {
              await db.collection("jobs").deleteOne({ UUID: existingJob.UUID });
            });
            deletedJobsCount++;
            continue;
          }

          // Update job using Workiz API
          console.log(`🔄 Updating job: ${existingJob.UUID}`);
          const updateUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/get/${existingJob.UUID}/`;

          const updateResponse = await RetryHandler.withRetry(
            async () => {
              const resp = await APIManager.fetchWithTimeout(
                updateUrl,
                {},
                30000
              );

              if (!resp.ok) {
                const errorText = await resp.text();
                console.log(
                  `❌ Job update error: ${resp.status} - ${errorText}`
                );

                // Check if response is HTML (520 error page)
                if (
                  errorText.includes('<div class="text-container">') ||
                  errorText.includes("Oops!") ||
                  errorText.includes("Something went wrong")
                ) {
                  console.log(
                    `🚨 Detected HTML error page from Workiz API (likely 520 error)`
                  );
                  throw new Error(
                    `Workiz API 520 error - server is experiencing issues`
                  );
                }

                throw new Error(
                  `Job update error: ${resp.status} - ${errorText}`
                );
              }

              return resp;
            },
            3,
            1000,
            workizCircuitBreaker
          ); // 3 retries, 1s base delay, with circuit breaker

          if (updateResponse.ok) {
            const updateData = await updateResponse.json();

            if (updateData.flag && updateData.data) {
              // Update the job with fresh data from Workiz
              const updatedJob = {
                ...updateData.data,
                accountId: account._id || account.id,
              };

              await RetryHandler.withRetry(async () => {
                await db
                  .collection("jobs")
                  .updateOne({ UUID: existingJob.UUID }, { $set: updatedJob });
              });

              updatedJobsCount++;
              console.log(`✅ Updated job: ${existingJob.UUID}`);
            } else {
              console.log(
                `⚠️ Job not found in Workiz API: ${existingJob.UUID}`
              );
              // Job might have been deleted in Workiz, so delete from our database
              await RetryHandler.withRetry(async () => {
                await db
                  .collection("jobs")
                  .deleteOne({ UUID: existingJob.UUID });
              });
              deletedJobsCount++;
            }
          } else {
            console.log(
              `❌ Failed to update job ${existingJob.UUID}: ${updateResponse.status}`
            );
            failedUpdatesCount++;
          }

          // Add a small delay between individual job updates (100ms)
          await new Promise((resolve) => setTimeout(resolve, 100));
        } catch (error) {
          console.log(
            `❌ Error processing job ${existingJob.UUID}: ${error.message}`
          );
          failedUpdatesCount++;
        }
      }

      // Add delay between batches (except for the last batch)
      if (i + BATCH_SIZE < existingJobs.length) {
        console.log(`⏳ Waiting 60 seconds before next batch...`);
        await new Promise((resolve) =>
          setTimeout(resolve, DELAY_BETWEEN_BATCHES)
        );
      }
    }

    console.log(`📊 Job update and cleanup completed:`);
    console.log(`   - Updated: ${updatedJobsCount} jobs`);
    console.log(`   - Deleted (old): ${deletedJobsCount} jobs`);
    console.log(`   - Failed updates: ${failedUpdatesCount} jobs`);

    // Record sync history
    const syncHistoryRecord = {
      accountId: account._id || account.id,
      syncType: "jobs",
      status: "success",
      timestamp: new Date(),
      duration: Date.now() - accountStartTime,
      details: {
        jobsFromWorkiz: jobs.length,
        existingJobsFound: existingJobs.length,
        finalJobCount: finalJobCount,
        jobsUpdated: updatedJobsCount,
        jobsDeleted: deletedJobsCount,
        failedUpdates: failedUpdatesCount,
        syncMethod: "manual",
        jobStatusBreakdown: {
          submitted: jobs.filter((j) => j.Status === "Submitted").length,
          pending: jobs.filter((j) => j.Status === "Pending").length,
          completed: jobs.filter(
            (j) =>
              j.Status === "Completed" || j.Status === "done pending approval"
          ).length,
          cancelled: jobs.filter((j) =>
            ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
              j.Status
            )
          ).length,
        },
      },
    };

    await RetryHandler.withRetry(async () => {
      await db.collection("syncHistory").insertOne(syncHistoryRecord);
    });

    // Update account's lastSyncDate
    await RetryHandler.withRetry(async () => {
      await db
        .collection("accounts")
        .updateOne(
          { _id: account._id || new ObjectId(account.id) },
          { $set: { lastSyncDate: new Date() } }
        );
    });

    res.json({
      message: `Synced ${jobs.length} jobs for account ${
        account.name || "Unknown"
      }`,
      details: {
        jobsFromWorkiz: jobs.length,
        existingJobsFound: existingJobs.length,
        finalJobCount: finalJobCount,
        jobsUpdated: updatedJobsCount,
        jobsDeleted: deletedJobsCount,
        failedUpdates: failedUpdatesCount,
      },
    });
  } catch (error) {
    console.log(`❌ Sync error: ${error.message}`);

    // Check if it's a circuit breaker error
    if (error.message.includes("Circuit breaker is OPEN")) {
      const workizState = workizCircuitBreaker.getState();
      console.log(
        `🚨 Circuit breaker blocked sync: ${workizState.state} state, ${workizState.failureCount} failures`
      );

      return res.status(503).json({
        error: "Service temporarily unavailable due to API issues",
        details: {
          circuitBreakerState: workizState.state,
          failureCount: workizState.failureCount,
          timeUntilRecovery: workizState.timeUntilRecovery,
          message: "Workiz API is experiencing issues. Please try again later.",
        },
      });
    }

    // Record failed sync history if we have account info
    if (req.params.accountId) {
      try {
        const syncHistoryRecord = {
          accountId: req.params.accountId,
          syncType: "jobs",
          status: "error",
          timestamp: new Date(),
          duration: Date.now() - accountStartTime,
          errorMessage: error.message,
          details: {},
        };
        await db.collection("syncHistory").insertOne(syncHistoryRecord);
        console.log(`📝 Failed sync history recorded for jobs sync`);
      } catch (historyError) {
        console.log(
          `❌ Failed to record sync history: ${historyError.message}`
        );
      }
    }

    res.status(500).json({ error: error.message });
  }
});

// Google Sheets integration endpoint
app.post("/api/sync-to-sheets/:accountId", async (req, res) => {
  const accountStartTime = Date.now();

  try {
    const { accountId } = req.params;
    console.log(`📊 Starting Google Sheets sync for account ID: ${accountId}`);

    // Find account by ID - try both id and _id fields
    const account = await db.collection("accounts").findOne({
      $or: [{ _id: new ObjectId(accountId) }, { id: accountId }],
    });

    if (!account) {
      console.log(`❌ Account not found for ID: ${accountId}`);
      return res.status(404).json({ error: "Account not found" });
    }

    console.log(`✅ Found account: ${account.name}`);
    console.log(
      `📋 Account sourceFilter: ${JSON.stringify(account.sourceFilter)}`
    );
    console.log(
      `💰 Default conversion value: ${account.defaultConversionValue}`
    );

    if (!account.googleSheetsId) {
      console.log(`❌ Missing Google Sheet ID for account: ${account.name}`);
      return res
        .status(400)
        .json({ error: "Missing Google Sheet ID for this account" });
    }

    console.log(`📄 Google Sheet ID: ${account.googleSheetsId}`);

    // Get all jobs for this account
    const allJobs = await db
      .collection("jobs")
      .find({ accountId: account._id || account.id })
      .toArray();

    console.log(`📊 Found ${allJobs.length} total jobs for account`);

    // Filter jobs by sourceFilter
    let filteredJobs = allJobs;
    if (
      account.sourceFilter &&
      Array.isArray(account.sourceFilter) &&
      account.sourceFilter.length > 0
    ) {
      filteredJobs = allJobs.filter((job) =>
        account.sourceFilter.includes(job.JobSource)
      );
      console.log(
        `🔍 Filtered jobs by sourceFilter: ${allJobs.length} → ${filteredJobs.length} jobs`
      );
      console.log(
        `📋 Job sources found: ${[
          ...new Set(filteredJobs.map((job) => job.JobSource)),
        ].join(", ")}`
      );
    } else {
      console.log(
        `⚠️ No sourceFilter configured, using all ${allJobs.length} jobs`
      );
    }

    if (filteredJobs.length === 0) {
      console.log(`⚠️ No jobs match the sourceFilter criteria`);
      return res.json({
        message: `No jobs to sync for account ${account.name}`,
        details: {
          totalJobs: allJobs.length,
          filteredJobs: 0,
          sourceFilter: account.sourceFilter,
        },
      });
    }

    // Parse Google Sheets credentials
    let credentials;
    try {
      // Try to get credentials from VITE_ prefixed variable first
      const credentialsStr =
        process.env.VITE_GOOGLE_SHEETS_CREDENTIALS ||
        process.env.GOOGLE_SHEETS_CREDENTIALS;

      if (!credentialsStr) {
        throw new Error(
          "Google Sheets credentials not found in environment variables"
        );
      }

      credentials = JSON.parse(credentialsStr);
      console.log(`✅ Google Sheets credentials parsed successfully`);
    } catch (error) {
      console.error("❌ Error parsing Google Sheets credentials:", error);
      return res.status(500).json({
        error: "Invalid Google Sheets credentials format",
        details: error.message,
      });
    }

    // Initialize Google Sheets client
    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    const sheets = google.sheets({ version: "v4", auth });
    console.log(`🔐 Google Sheets client initialized`);

    // Clear the sheet first (skip header row)
    console.log(`🧹 Clearing existing data from sheet (preserving headers)...`);
    try {
      await RetryHandler.withRetry(
        async () => {
          await sheets.spreadsheets.values.clear({
            spreadsheetId: account.googleSheetsId,
            range: "Sheet1!A2:F", // Start from row 2 to preserve headers
          });
        },
        3,
        1000,
        sheetsCircuitBreaker
      );
      console.log(`✅ Sheet cleared successfully (headers preserved)`);
    } catch (error) {
      console.log(`❌ Error clearing sheet: ${error.message}`);
      return res.status(500).json({
        error: "Failed to clear Google Sheet",
        details: error.message,
      });
    }

    // Prepare data for Google Sheets
    console.log(
      `📝 Preparing ${filteredJobs.length} jobs for Google Sheets...`
    );
    const values = filteredJobs.map((job, index) => {
      const formattedTime =
        formatInTimeZone(
          new Date(job.JobDateTime),
          "America/Los_Angeles",
          "yyyy-MM-dd'T'HH:mm:ss"
        ) + " America/Los_Angeles";

      if (index < 3) {
        console.log(
          `📋 Sample job ${index + 1}: ${job.Phone} | ${formattedTime} | ${
            job.JobSource
          }`
        );
      }

      // New conversion value logic
      let conversionValue = account.defaultConversionValue || 0;

      // If JobTotalPrice has a value and is not 0, use it
      if (job.JobTotalPrice && job.JobTotalPrice !== 0) {
        conversionValue = job.JobTotalPrice;
      }

      // If Status is cancelled (case-insensitive), set to 0
      if (
        job.Status &&
        ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(job.Status)
      ) {
        conversionValue = 0;
      }

      return [
        job.Phone || "", // Caller's Phone Number
        formattedTime, // Call Start Time
        "Google Ads Convert", // Conversion Name
        "", // Conversion Time (blank)
        conversionValue, // Conversion Value
        "USD", // Conversion Currency
      ];
    });

    console.log(`📊 Prepared ${values.length} rows for Google Sheets`);

    // Add to Google Sheet (starting from row 2 to preserve headers)
    console.log(`📤 Adding data to Google Sheet (starting from row 2)...`);
    const response = await RetryHandler.withRetry(
      async () => {
        return await sheets.spreadsheets.values.append({
          spreadsheetId: account.googleSheetsId,
          range: "Sheet1!A2:F", // Start from row 2 to preserve headers
          valueInputOption: "USER_ENTERED",
          requestBody: {
            values,
          },
        });
      },
      3,
      1000,
      sheetsCircuitBreaker
    );

    console.log(`✅ Google Sheets sync completed successfully`);
    console.log(`📈 Updated rows: ${response.data.updates?.updatedRows || 0}`);

    // Record sync history
    const syncHistoryRecord = {
      accountId: account._id || account.id,
      syncType: "sheets",
      status: "success",
      timestamp: new Date(),
      duration: Date.now() - accountStartTime,
      details: {
        totalJobs: allJobs.length,
        filteredJobs: filteredJobs.length,
        updatedRows: response.data.updates?.updatedRows || 0,
        sourceFilter: account.sourceFilter,
        sampleJobSources: [
          ...new Set(filteredJobs.slice(0, 5).map((job) => job.JobSource)),
        ],
        syncMethod: "manual",
        jobStatusBreakdown: {
          submitted: filteredJobs.filter((j) => j.Status === "Submitted")
            .length,
          pending: filteredJobs.filter((j) => j.Status === "Pending").length,
          completed: filteredJobs.filter(
            (j) =>
              j.Status === "Completed" || j.Status === "done pending approval"
          ).length,
          cancelled: filteredJobs.filter((j) =>
            ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
              j.Status
            )
          ).length,
        },
        conversionValueLogic: {
          defaultValue: account.defaultConversionValue || 0,
          jobsWithJobTotalPrice: filteredJobs.filter(
            (j) => j.JobTotalPrice && j.JobTotalPrice !== 0
          ).length,
          jobsWithCancelledStatus: filteredJobs.filter((j) =>
            ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
              j.Status
            )
          ).length,
          totalConversionValue: values.reduce(
            (sum, row) => sum + (row[4] || 0),
            0
          ),
        },
      },
    };

    await RetryHandler.withRetry(async () => {
      await db.collection("syncHistory").insertOne(syncHistoryRecord);
    });

    const accountDuration = Date.now() - accountStartTime;
    console.log(
      `📊 Google Sheets sync summary for ${account.name} (${accountDuration}ms):`
    );
    console.log(`   - Total jobs: ${allJobs.length}`);
    console.log(`   - Filtered jobs: ${filteredJobs.length}`);
    console.log(
      `   - Updated rows: ${response.data.updates?.updatedRows || 0}`
    );

    // Update account's lastSyncDate
    await RetryHandler.withRetry(async () => {
      await db
        .collection("accounts")
        .updateOne(
          { _id: account._id || new ObjectId(account.id) },
          { $set: { lastSyncDate: new Date() } }
        );
    });

    res.json({
      message: `Synced ${filteredJobs.length} jobs to Google Sheets for account ${account.name}`,
      details: {
        totalJobs: allJobs.length,
        filteredJobs: filteredJobs.length,
        updatedRows: response.data.updates?.updatedRows || 0,
        sourceFilter: account.sourceFilter,
        sampleJobSources: [
          ...new Set(filteredJobs.slice(0, 5).map((job) => job.JobSource)),
        ],
        syncMethod: "manual",
        jobStatusBreakdown: {
          submitted: filteredJobs.filter((j) => j.Status === "Submitted")
            .length,
          pending: filteredJobs.filter((j) => j.Status === "Pending").length,
          completed: filteredJobs.filter(
            (j) =>
              j.Status === "Completed" || j.Status === "done pending approval"
          ).length,
          cancelled: filteredJobs.filter((j) =>
            ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
              j.Status
            )
          ).length,
        },
        conversionValueLogic: {
          defaultValue: account.defaultConversionValue || 0,
          jobsWithJobTotalPrice: filteredJobs.filter(
            (j) => j.JobTotalPrice && j.JobTotalPrice !== 0
          ).length,
          jobsWithCancelledStatus: filteredJobs.filter((j) =>
            ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
              j.Status
            )
          ).length,
          totalConversionValue: values.reduce(
            (sum, row) => sum + (row[4] || 0),
            0
          ),
        },
      },
    });
  } catch (error) {
    console.error("❌ Error syncing to Google Sheets:", error);

    // Record failed sync history if we have account info
    if (req.params.accountId) {
      try {
        const syncHistoryRecord = {
          accountId: req.params.accountId,
          syncType: "sheets",
          status: "error",
          timestamp: new Date(),
          duration: Date.now() - accountStartTime,
          errorMessage: error.message,
          details: {},
        };
        await db.collection("syncHistory").insertOne(syncHistoryRecord);
        console.log(`📝 Failed sync history recorded for sheets sync`);
      } catch (historyError) {
        console.log(
          `❌ Failed to record sync history: ${historyError.message}`
        );
      }
    }

    res.status(500).json({
      error: error.message,
      details: error.response?.data || "Unknown error occurred",
    });
  }
});

// Manual trigger endpoint for testing cron functionality
app.post("/api/trigger-sync/:accountId", async (req, res) => {
  try {
    const { accountId } = req.params;
    console.log(`🔧 Manual sync trigger for account ID: ${accountId}`);

    // Find account
    const account = await db.collection("accounts").findOne({
      $or: [{ _id: new ObjectId(accountId) }, { id: accountId }],
    });

    if (!account) {
      return res.status(404).json({ error: "Account not found" });
    }

    console.log(`⏰ Manual sync for account: ${account.name}`);

    // Sync jobs
    try {
      const startDate = getStartDate();
      const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/all/?start_date=${startDate}&offset=0&records=100&only_open=false`;
      const response = await fetch(workizUrl);

      if (!response.ok) {
        throw new Error(`Workiz API error: ${response.status}`);
      }

      const data = await response.json();
      if (!data.flag || !Array.isArray(data.data)) {
        throw new Error("Invalid response from Workiz API");
      }

      // Add accountId to each job
      const jobs = data.data.map((job) => ({
        ...job,
        accountId: account._id || account.id,
      }));

      // Upsert jobs into MongoDB
      const bulkOps = jobs.map((job) => ({
        updateOne: {
          filter: { UUID: job.UUID },
          update: { $set: job },
          upsert: true,
        },
      }));

      if (bulkOps.length > 0) {
        const bulkResult = await db.collection("jobs").bulkWrite(bulkOps);
        console.log(
          `✅ Jobs sync completed: ${bulkResult.upsertedCount} new, ${bulkResult.modifiedCount} updated`
        );
      }

      // Update existing jobs and clean up old ones
      console.log(`🔄 Starting job update and cleanup process...`);

      // Get all existing jobs for this account
      const existingJobs = await db
        .collection("jobs")
        .find({ accountId: account._id || account.id })
        .toArray();

      console.log(`📋 Found ${existingJobs.length} existing jobs in database`);

      // Calculate 1-year cutoff date
      const oneYearAgo = new Date();
      oneYearAgo.setDate(oneYearAgo.getDate() - 365);

      let updatedJobsCount = 0;
      let deletedJobsCount = 0;
      let failedUpdatesCount = 0;

      // Process jobs in batches of 29 with 60-second delays
      const BATCH_SIZE = 29;
      const DELAY_BETWEEN_BATCHES = 60000; // 60 seconds in milliseconds

      for (let i = 0; i < existingJobs.length; i += BATCH_SIZE) {
        const batch = existingJobs.slice(i, i + BATCH_SIZE);
        const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
        const totalBatches = Math.ceil(existingJobs.length / BATCH_SIZE);

        console.log(
          `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} jobs)`
        );

        // Process each job in the current batch
        for (const existingJob of batch) {
          try {
            const jobDate = new Date(existingJob.JobDateTime);

            // Check if job is older than 1 year
            if (jobDate < oneYearAgo) {
              console.log(
                `🗑️ Deleting old job: ${existingJob.UUID} (${existingJob.JobDateTime})`
              );
              await RetryHandler.withRetry(async () => {
                await db
                  .collection("jobs")
                  .deleteOne({ UUID: existingJob.UUID });
              });
              deletedJobsCount++;
              continue;
            }

            // Update job using Workiz API
            console.log(`🔄 Updating job: ${existingJob.UUID}`);
            const updateUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/get/${existingJob.UUID}/`;

            const updateResponse = await RetryHandler.withRetry(
              async () => {
                const resp = await APIManager.fetchWithTimeout(
                  updateUrl,
                  {},
                  30000
                );

                if (!resp.ok) {
                  const errorText = await resp.text();
                  console.log(
                    `❌ Job update error: ${resp.status} - ${errorText}`
                  );

                  // Check if response is HTML (520 error page)
                  if (
                    errorText.includes('<div class="text-container">') ||
                    errorText.includes("Oops!") ||
                    errorText.includes("Something went wrong")
                  ) {
                    console.log(
                      `🚨 Detected HTML error page from Workiz API (likely 520 error)`
                    );
                    throw new Error(
                      `Workiz API 520 error - server is experiencing issues`
                    );
                  }

                  throw new Error(
                    `Job update error: ${resp.status} - ${errorText}`
                  );
                }

                return resp;
              },
              3,
              1000,
              workizCircuitBreaker
            ); // 3 retries, 1s base delay, with circuit breaker

            if (updateResponse.ok) {
              const updateData = await updateResponse.json();

              if (updateData.flag && updateData.data) {
                // Update the job with fresh data from Workiz
                const updatedJob = {
                  ...updateData.data,
                  accountId: account._id || account.id,
                };

                await RetryHandler.withRetry(async () => {
                  await db
                    .collection("jobs")
                    .updateOne(
                      { UUID: existingJob.UUID },
                      { $set: updatedJob }
                    );
                });

                updatedJobsCount++;
                console.log(`✅ Updated job: ${existingJob.UUID}`);
              } else {
                console.log(
                  `⚠️ Job not found in Workiz API: ${existingJob.UUID}`
                );
                // Job might have been deleted in Workiz, so delete from our database
                await RetryHandler.withRetry(async () => {
                  await db
                    .collection("jobs")
                    .deleteOne({ UUID: existingJob.UUID });
                });
                deletedJobsCount++;
              }
            } else {
              console.log(
                `❌ Failed to update job ${existingJob.UUID}: ${updateResponse.status}`
              );
              failedUpdatesCount++;
            }

            // Add a small delay between individual job updates (100ms)
            await new Promise((resolve) => setTimeout(resolve, 100));
          } catch (error) {
            console.log(
              `❌ Error processing job ${existingJob.UUID}: ${error.message}`
            );
            failedUpdatesCount++;
          }
        }

        // Add delay between batches (except for the last batch)
        if (i + BATCH_SIZE < existingJobs.length) {
          console.log(`⏳ Waiting 60 seconds before next batch...`);
          await new Promise((resolve) =>
            setTimeout(resolve, DELAY_BETWEEN_BATCHES)
          );
        }
      }

      console.log(`📊 Job update and cleanup completed:`);
      console.log(`   - Updated: ${updatedJobsCount} jobs`);
      console.log(`   - Deleted (old): ${deletedJobsCount} jobs`);
      console.log(`   - Failed updates: ${failedUpdatesCount} jobs`);

      // Record sync history
      const syncHistoryRecord = {
        accountId: account._id || account.id,
        syncType: "jobs",
        status: "success",
        timestamp: new Date(),
        duration: Date.now() - accountStartTime,
        details: {
          jobsFromWorkiz: jobs.length,
          existingJobsFound: existingJobs.length,
          finalJobCount: await db
            .collection("jobs")
            .countDocuments({ accountId: account._id || account.id }),
          jobsUpdated: updatedJobsCount,
          jobsDeleted: deletedJobsCount,
          failedUpdates: failedUpdatesCount,
          syncMethod: "manual",
          jobStatusBreakdown: {
            submitted: jobs.filter((j) => j.Status === "Submitted").length,
            pending: jobs.filter((j) => j.Status === "Pending").length,
            completed: jobs.filter(
              (j) =>
                j.Status === "Completed" || j.Status === "done pending approval"
            ).length,
            cancelled: jobs.filter((j) =>
              ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
                j.Status
              )
            ).length,
          },
        },
      };

      await RetryHandler.withRetry(async () => {
        await db.collection("syncHistory").insertOne(syncHistoryRecord);
      });

      // Update account's lastSyncDate
      await RetryHandler.withRetry(async () => {
        await db
          .collection("accounts")
          .updateOne(
            { _id: account._id || new ObjectId(account.id) },
            { $set: { lastSyncDate: new Date() } }
          );
      });

      res.json({
        message: `Manual sync completed for account ${account.name}`,
        jobsSync: {
          success: true,
          jobsSynced: jobs.length,
          jobsUpdated: updatedJobsCount,
          jobsDeleted: deletedJobsCount,
          failedUpdates: failedUpdatesCount,
        },
      });
    } catch (error) {
      console.error(
        `❌ Manual sync failed for account ${account.name}:`,
        error.message
      );

      // Record failed sync history
      const syncHistoryRecord = {
        accountId: account._id || account.id,
        syncType: "jobs",
        status: "error",
        timestamp: new Date(),
        duration: Date.now() - accountStartTime,
        errorMessage: error.message,
        details: {},
      };
      await RetryHandler.withRetry(async () => {
        await db.collection("syncHistory").insertOne(syncHistoryRecord);
      });

      res.status(500).json({
        error: error.message,
        jobsSync: { success: false, error: error.message },
      });
    }
  } catch (error) {
    console.error("❌ Manual trigger failed:", error);
    res.status(500).json({ error: error.message });
  }
});

// Cron job endpoint
app.get("/api/cron/sync-jobs", async (req, res) => {
  try {
    // Enhanced security validation
    const userAgent = req.get("User-Agent");
    const clientIP = req.ip || req.connection.remoteAddress;

    if (!userAgent || !userAgent.includes("vercel-cron")) {
      console.log(`❌ Unauthorized cron access attempt:`, {
        userAgent,
        clientIP,
        timestamp: new Date().toISOString(),
      });
      return res.status(403).json({
        error: "Unauthorized",
        timestamp: new Date().toISOString(),
      });
    }

    console.log(`🕐 Vercel Cron Job triggered at: ${new Date().toISOString()}`);
    console.log(`📊 Starting enhanced sync process...`);

    // Ensure database connection with health check
    const db = await ensureDbConnection();
    await DatabaseManager.ensureHealthyConnection(db);

    // Get all accounts with enhanced error handling
    const accounts = await RetryHandler.withRetry(async () => {
      const result = await db.collection("accounts").find({}).toArray();
      return result;
    });

    if (accounts.length === 0) {
      console.log("📭 No accounts found to sync");
      return res.json({
        message: "No accounts found to sync",
      });
    }

    console.log(`📋 Found ${accounts.length} accounts to sync`);

    const syncResults = [];
    const startTime = Date.now();

    // Process accounts with enhanced error handling
    for (const [index, account] of accounts.entries()) {
      const accountStartTime = Date.now();
      console.log(
        `⏰ Processing account ${index + 1}/${accounts.length}: ${account.name}`
      );

      try {
        // Enhanced API call with timeout and retry
        const startDate = getStartDate();
        const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/all/?start_date=${startDate}&offset=0&records=100&only_open=false`;

        const response = await RetryHandler.withRetry(
          async () => {
            const resp = await APIManager.fetchWithTimeout(
              workizUrl,
              {},
              45000
            );

            if (!resp.ok) {
              const errorText = await resp.text();
              console.log(`❌ Workiz API error: ${resp.status} - ${errorText}`);
              throw new Error(
                `Workiz API error: ${resp.status} - ${errorText}`
              );
            }

            return resp;
          },
          5,
          2000,
          workizCircuitBreaker
        ); // 5 retries, 2s base delay, with circuit breaker

        const data = await response.json();
        if (!data.flag || !Array.isArray(data.data)) {
          throw new Error("Invalid response structure from Workiz API");
        }

        // Add accountId to each job
        const jobs = data.data.map((job) => ({
          ...job,
          accountId: account._id || account.id,
        }));

        // Enhanced bulk operations with error handling
        const bulkOps = jobs.map((job) => ({
          updateOne: {
            filter: { UUID: job.UUID },
            update: { $set: job },
            upsert: true,
          },
        }));

        if (bulkOps.length > 0) {
          const bulkResult = await RetryHandler.withRetry(async () => {
            return await db.collection("jobs").bulkWrite(bulkOps);
          });

          console.log(
            `✅ Jobs sync completed for ${account.name}: ${bulkResult.upsertedCount} new, ${bulkResult.modifiedCount} updated`
          );
        }

        // Enhanced job update and cleanup process
        console.log(
          `🔄 Starting job update and cleanup process for ${account.name}...`
        );

        const accountId = account._id || account.id;
        const existingJobs = await RetryHandler.withRetry(async () => {
          return await db.collection("jobs").find({ accountId }).toArray();
        });

        console.log(
          `📋 Found ${existingJobs.length} existing jobs in database`
        );

        // Calculate 1-year cutoff date
        const oneYearAgo = new Date();
        oneYearAgo.setDate(oneYearAgo.getDate() - 365);

        let updatedJobsCount = 0;
        let deletedJobsCount = 0;
        let failedUpdatesCount = 0;

        // Process jobs in batches to avoid memory issues and rate limiting
        const BATCH_SIZE = 29;
        const DELAY_BETWEEN_BATCHES = 60000; // 60 seconds in milliseconds

        for (let i = 0; i < existingJobs.length; i += BATCH_SIZE) {
          const batch = existingJobs.slice(i, i + BATCH_SIZE);
          const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
          const totalBatches = Math.ceil(existingJobs.length / BATCH_SIZE);

          console.log(
            `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} jobs)`
          );

          // Process each job in the current batch
          for (const existingJob of batch) {
            try {
              const jobDate = new Date(existingJob.JobDateTime);

              // Check if job is older than 1 year
              if (jobDate < oneYearAgo) {
                console.log(
                  `🗑️ Deleting old job: ${existingJob.UUID} (${existingJob.JobDateTime})`
                );
                await RetryHandler.withRetry(async () => {
                  await db
                    .collection("jobs")
                    .deleteOne({ UUID: existingJob.UUID });
                });
                deletedJobsCount++;
                continue;
              }

              // Update job using Workiz API
              console.log(`🔄 Updating job: ${existingJob.UUID}`);
              const updateUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/get/${existingJob.UUID}/`;

              const updateResponse = await RetryHandler.withRetry(
                async () => {
                  const resp = await APIManager.fetchWithTimeout(
                    updateUrl,
                    {},
                    30000
                  );

                  if (!resp.ok) {
                    const errorText = await resp.text();
                    console.log(
                      `❌ Job update error: ${resp.status} - ${errorText}`
                    );

                    // Check if response is HTML (520 error page)
                    if (
                      errorText.includes('<div class="text-container">') ||
                      errorText.includes("Oops!") ||
                      errorText.includes("Something went wrong")
                    ) {
                      console.log(
                        `🚨 Detected HTML error page from Workiz API (likely 520 error)`
                      );
                      throw new Error(
                        `Workiz API 520 error - server is experiencing issues`
                      );
                    }

                    throw new Error(
                      `Job update error: ${resp.status} - ${errorText}`
                    );
                  }

                  return resp;
                },
                3,
                1000,
                workizCircuitBreaker
              ); // 3 retries, 1s base delay, with circuit breaker

              if (updateResponse.ok) {
                const updateData = await updateResponse.json();

                if (updateData.flag && updateData.data) {
                  // Update the job with fresh data from Workiz
                  const updatedJob = {
                    ...updateData.data,
                    accountId: account._id || account.id,
                  };

                  await RetryHandler.withRetry(async () => {
                    await db
                      .collection("jobs")
                      .updateOne(
                        { UUID: existingJob.UUID },
                        { $set: updatedJob }
                      );
                  });

                  updatedJobsCount++;
                  console.log(`✅ Updated job: ${existingJob.UUID}`);
                } else {
                  console.log(
                    `⚠️ Job not found in Workiz API: ${existingJob.UUID}`
                  );
                  // Job might have been deleted in Workiz, so delete from our database
                  await RetryHandler.withRetry(async () => {
                    await db
                      .collection("jobs")
                      .deleteOne({ UUID: existingJob.UUID });
                  });
                  deletedJobsCount++;
                }
              } else {
                console.log(
                  `❌ Failed to update job ${existingJob.UUID}: ${updateResponse.status}`
                );
                failedUpdatesCount++;
              }

              // Add a small delay between individual job updates (100ms)
              await new Promise((resolve) => setTimeout(resolve, 100));
            } catch (error) {
              console.log(
                `❌ Error processing job ${existingJob.UUID}: ${error.message}`
              );
              failedUpdatesCount++;
            }
          }

          // Add delay between batches (except for the last batch)
          if (i + BATCH_SIZE < existingJobs.length) {
            console.log(`⏳ Waiting 60 seconds before next batch...`);
            await new Promise((resolve) =>
              setTimeout(resolve, DELAY_BETWEEN_BATCHES)
            );
          }
        }

        const finalJobCount = await RetryHandler.withRetry(async () => {
          return await db.collection("jobs").countDocuments({ accountId });
        });

        const accountDuration = Date.now() - accountStartTime;
        console.log(
          `📊 Sync summary for ${account.name} (${accountDuration}ms):`
        );
        console.log(`   - Jobs from Workiz: ${jobs.length}`);
        console.log(`   - Updated: ${updatedJobsCount} jobs`);
        console.log(`   - Deleted (old): ${deletedJobsCount} jobs`);
        console.log(`   - Failed updates: ${failedUpdatesCount} jobs`);
        console.log(`   - Final job count: ${finalJobCount} jobs`);

        // Enhanced sync history recording
        const syncHistoryRecord = {
          accountId: account._id || account.id,
          syncType: "jobs",
          status: "success",
          timestamp: new Date(),
          duration: accountDuration,
          details: {
            jobsFromWorkiz: jobs.length,
            finalJobCount: finalJobCount,
            jobsUpdated: updatedJobsCount,
            jobsDeleted: deletedJobsCount,
            failedUpdates: failedUpdatesCount,
            syncMethod: "cron",
            jobStatusBreakdown: {
              submitted: jobs.filter((j) => j.Status === "Submitted").length,
              pending: jobs.filter((j) => j.Status === "Pending").length,
              completed: jobs.filter(
                (j) =>
                  j.Status === "Completed" ||
                  j.Status === "done pending approval"
              ).length,
              cancelled: jobs.filter((j) =>
                ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
                  j.Status
                )
              ).length,
            },
          },
        };

        await RetryHandler.withRetry(async () => {
          await db.collection("syncHistory").insertOne(syncHistoryRecord);
        });

        // Update account's lastSyncDate
        await RetryHandler.withRetry(async () => {
          await db
            .collection("accounts")
            .updateOne(
              { _id: account._id || new ObjectId(account.id) },
              { $set: { lastSyncDate: new Date() } }
            );
        });

        syncResults.push({
          account: account.name,
          success: true,
          duration: accountDuration,
          jobsSynced: jobs.length,
          jobsUpdated: updatedJobsCount,
          jobsDeleted: deletedJobsCount,
          failedUpdates: failedUpdatesCount,
        });
      } catch (error) {
        const accountDuration = Date.now() - accountStartTime;
        console.log(
          `❌ Sync failed for account ${account.name} (${accountDuration}ms):`,
          error.message
        );

        // Enhanced failed sync history recording
        const syncHistoryRecord = {
          accountId: account._id || account.id,
          syncType: "jobs",
          status: "error",
          timestamp: new Date(),
          duration: accountDuration,
          errorMessage: error.message,
          errorStack: error.stack,
          details: {},
        };

        try {
          await db.collection("syncHistory").insertOne(syncHistoryRecord);
        } catch (historyError) {
          console.error(
            "❌ Failed to record sync history:",
            historyError.message
          );
        }

        syncResults.push({
          account: account.name,
          success: false,
          duration: accountDuration,
          error: error.message,
        });
      }
    }

    const totalDuration = Date.now() - startTime;
    const successfulSyncs = syncResults.filter((r) => r.success).length;
    const failedSyncs = syncResults.filter((r) => !r.success).length;

    console.log(`🎯 Enhanced cron job completed in ${totalDuration}ms:`);
    console.log(`   - Successful: ${successfulSyncs} accounts`);
    console.log(`   - Failed: ${failedSyncs} accounts`);

    res.json({
      message: `Enhanced cron job completed: ${successfulSyncs} successful, ${failedSyncs} failed`,
      duration: totalDuration,
      results: syncResults,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const duration = Date.now() - startTime;
    console.log(
      `❌ Enhanced cron job error after ${duration}ms: ${error.message}`
    );
    console.error("Full error:", error);

    res.status(500).json({
      error: error.message,
      duration: duration,
      timestamp: new Date().toISOString(),
    });
  }
});

// Cron job endpoint for Google Sheets sync
app.get("/api/cron/sync-sheets", async (req, res) => {
  try {
    // Enhanced security validation
    const userAgent = req.get("User-Agent");
    const clientIP = req.ip || req.connection.remoteAddress;

    if (!userAgent || !userAgent.includes("vercel-cron")) {
      console.log(`❌ Unauthorized cron access attempt:`, {
        userAgent,
        clientIP,
        timestamp: new Date().toISOString(),
      });
      return res.status(403).json({
        error: "Unauthorized",
        timestamp: new Date().toISOString(),
      });
    }

    console.log(
      `🕐 Vercel Cron Job for Google Sheets sync triggered at: ${new Date().toISOString()}`
    );
    console.log(`📊 Starting Google Sheets sync process...`);

    // Ensure database connection with health check
    const db = await ensureDbConnection();
    await DatabaseManager.ensureHealthyConnection(db);

    // Get all accounts with Google Sheets ID
    const accounts = await RetryHandler.withRetry(async () => {
      const result = await db
        .collection("accounts")
        .find({
          googleSheetsId: { $exists: true, $ne: "" },
        })
        .toArray();
      return result;
    });

    if (accounts.length === 0) {
      console.log("📭 No accounts with Google Sheets ID found to sync");
      return res.json({
        message: "No accounts with Google Sheets ID found to sync",
      });
    }

    console.log(
      `📋 Found ${accounts.length} accounts with Google Sheets ID to sync`
    );

    const syncResults = [];
    const startTime = Date.now();

    // Parse Google Sheets credentials once
    let credentials;
    try {
      const credentialsStr =
        process.env.VITE_GOOGLE_SHEETS_CREDENTIALS ||
        process.env.GOOGLE_SHEETS_CREDENTIALS;

      if (!credentialsStr) {
        throw new Error(
          "Google Sheets credentials not found in environment variables"
        );
      }

      credentials = JSON.parse(credentialsStr);
      console.log(`✅ Google Sheets credentials parsed successfully`);
    } catch (error) {
      console.error("❌ Error parsing Google Sheets credentials:", error);
      return res.status(500).json({
        error: "Invalid Google Sheets credentials format",
        details: error.message,
      });
    }

    // Initialize Google Sheets client
    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: ["https://www.googleapis.com/auth/spreadsheets"],
    });

    const sheets = google.sheets({ version: "v4", auth });
    console.log(`🔐 Google Sheets client initialized`);

    // Process accounts with enhanced error handling
    for (const [index, account] of accounts.entries()) {
      const accountStartTime = Date.now();
      console.log(
        `⏰ Processing Google Sheets sync for account ${index + 1}/${
          accounts.length
        }: ${account.name}`
      );

      try {
        // Get all jobs for this account
        const allJobs = await RetryHandler.withRetry(async () => {
          return await db
            .collection("jobs")
            .find({ accountId: account._id || account.id })
            .toArray();
        });

        console.log(
          `📊 Found ${allJobs.length} total jobs for account ${account.name}`
        );

        // Filter jobs by sourceFilter
        let filteredJobs = allJobs;
        if (
          account.sourceFilter &&
          Array.isArray(account.sourceFilter) &&
          account.sourceFilter.length > 0
        ) {
          filteredJobs = allJobs.filter((job) =>
            account.sourceFilter.includes(job.JobSource)
          );
          console.log(
            `🔍 Filtered jobs by sourceFilter: ${allJobs.length} → ${filteredJobs.length} jobs`
          );
        } else {
          console.log(
            `⚠️ No sourceFilter configured, using all ${allJobs.length} jobs`
          );
        }

        if (filteredJobs.length === 0) {
          console.log(
            `⚠️ No jobs match the sourceFilter criteria for ${account.name}`
          );
          syncResults.push({
            account: account.name,
            success: true,
            duration: Date.now() - accountStartTime,
            jobsSynced: 0,
            message: "No jobs to sync",
          });
          continue;
        }

        // Clear the sheet first (skip header row)
        console.log(
          `🧹 Clearing existing data from sheet for ${account.name}...`
        );
        await RetryHandler.withRetry(
          async () => {
            await sheets.spreadsheets.values.clear({
              spreadsheetId: account.googleSheetsId,
              range: "Sheet1!A2:F", // Start from row 2 to preserve headers
            });
          },
          3,
          1000,
          sheetsCircuitBreaker
        );
        console.log(`✅ Sheet cleared successfully for ${account.name}`);

        // Prepare data for Google Sheets with new conversion value logic
        console.log(
          `📝 Preparing ${filteredJobs.length} jobs for Google Sheets...`
        );
        const values = filteredJobs.map((job, index) => {
          const formattedTime =
            formatInTimeZone(
              new Date(job.JobDateTime),
              "America/Los_Angeles",
              "yyyy-MM-dd'T'HH:mm:ss"
            ) + " America/Los_Angeles";

          // New conversion value logic
          let conversionValue = account.defaultConversionValue || 0;

          // If JobTotalPrice has a value and is not 0, use it
          if (job.JobTotalPrice && job.JobTotalPrice !== 0) {
            conversionValue = job.JobTotalPrice;
          }

          // If Status is cancelled (case-insensitive), set to 0
          if (
            job.Status &&
            ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
              job.Status
            )
          ) {
            conversionValue = 0;
          }

          return [
            job.Phone || "", // Caller's Phone Number
            formattedTime, // Call Start Time
            "Google Ads Convert", // Conversion Name
            "", // Conversion Time (blank)
            conversionValue, // Conversion Value
            "USD", // Conversion Currency
          ];
        });

        console.log(`📊 Prepared ${values.length} rows for Google Sheets`);

        // Add to Google Sheet (starting from row 2 to preserve headers)
        console.log(`📤 Adding data to Google Sheet for ${account.name}...`);
        const response = await RetryHandler.withRetry(
          async () => {
            return await sheets.spreadsheets.values.append({
              spreadsheetId: account.googleSheetsId,
              range: "Sheet1!A2:F", // Start from row 2 to preserve headers
              valueInputOption: "USER_ENTERED",
              requestBody: {
                values,
              },
            });
          },
          3,
          1000,
          sheetsCircuitBreaker
        );

        console.log(
          `✅ Google Sheets sync completed successfully for ${account.name}`
        );
        console.log(
          `📈 Updated rows: ${response.data.updates?.updatedRows || 0}`
        );

        // Record sync history
        const syncHistoryRecord = {
          accountId: account._id || account.id,
          syncType: "sheets",
          status: "success",
          timestamp: new Date(),
          duration: Date.now() - accountStartTime,
          details: {
            totalJobs: allJobs.length,
            filteredJobs: filteredJobs.length,
            updatedRows: response.data.updates?.updatedRows || 0,
            sourceFilter: account.sourceFilter,
            sampleJobSources: [
              ...new Set(filteredJobs.slice(0, 5).map((job) => job.JobSource)),
            ],
            syncMethod: "cron",
            jobStatusBreakdown: {
              submitted: filteredJobs.filter((j) => j.Status === "Submitted")
                .length,
              pending: filteredJobs.filter((j) => j.Status === "Pending")
                .length,
              completed: filteredJobs.filter(
                (j) =>
                  j.Status === "Completed" ||
                  j.Status === "done pending approval"
              ).length,
              cancelled: filteredJobs.filter((j) =>
                ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
                  j.Status
                )
              ).length,
            },
            conversionValueLogic: {
              defaultValue: account.defaultConversionValue || 0,
              jobsWithJobTotalPrice: filteredJobs.filter(
                (j) => j.JobTotalPrice && j.JobTotalPrice !== 0
              ).length,
              jobsWithCancelledStatus: filteredJobs.filter((j) =>
                ["Cancelled", "Canceled", "cancelled", "CANCELLED"].includes(
                  j.Status
                )
              ).length,
              totalConversionValue: values.reduce(
                (sum, row) => sum + (row[4] || 0),
                0
              ),
            },
          },
        };

        await RetryHandler.withRetry(async () => {
          await db.collection("syncHistory").insertOne(syncHistoryRecord);
        });

        const accountDuration = Date.now() - accountStartTime;
        console.log(
          `📊 Google Sheets sync summary for ${account.name} (${accountDuration}ms):`
        );
        console.log(`   - Total jobs: ${allJobs.length}`);
        console.log(`   - Filtered jobs: ${filteredJobs.length}`);
        console.log(
          `   - Updated rows: ${response.data.updates?.updatedRows || 0}`
        );

        syncResults.push({
          account: account.name,
          success: true,
          duration: accountDuration,
          jobsSynced: filteredJobs.length,
          updatedRows: response.data.updates?.updatedRows || 0,
        });
      } catch (error) {
        const accountDuration = Date.now() - accountStartTime;
        console.log(
          `❌ Google Sheets sync failed for account ${account.name} (${accountDuration}ms):`,
          error.message
        );

        // Enhanced failed sync history recording
        const syncHistoryRecord = {
          accountId: account._id || account.id,
          syncType: "sheets",
          status: "error",
          timestamp: new Date(),
          duration: accountDuration,
          errorMessage: error.message,
          errorStack: error.stack,
          details: {},
        };

        try {
          await db.collection("syncHistory").insertOne(syncHistoryRecord);
        } catch (historyError) {
          console.error(
            "❌ Failed to record sync history:",
            historyError.message
          );
        }

        syncResults.push({
          account: account.name,
          success: false,
          duration: accountDuration,
          error: error.message,
        });
      }
    }

    const totalDuration = Date.now() - startTime;
    const successfulSyncs = syncResults.filter((r) => r.success).length;
    const failedSyncs = syncResults.filter((r) => !r.success).length;

    console.log(`🎯 Google Sheets cron job completed in ${totalDuration}ms:`);
    console.log(`   - Successful: ${successfulSyncs} accounts`);
    console.log(`   - Failed: ${failedSyncs} accounts`);

    res.json({
      message: `Google Sheets cron job completed: ${successfulSyncs} successful, ${failedSyncs} failed`,
      duration: totalDuration,
      results: syncResults,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const duration = Date.now() - startTime;
    console.log(
      `❌ Google Sheets cron job error after ${duration}ms: ${error.message}`
    );
    console.error("Full error:", error);

    res.status(500).json({
      error: error.message,
      duration: duration,
      timestamp: new Date().toISOString(),
    });
  }
});

// Enhanced monitoring and health check endpoints
app.get("/api/health", async (req, res) => {
  try {
    const db = await ensureDbConnection();
    const dbHealthy = await DatabaseManager.healthCheck(db);

    const healthStatus = {
      status: dbHealthy ? "healthy" : "unhealthy",
      timestamp: new Date().toISOString(),
      services: {
        database: dbHealthy ? "connected" : "disconnected",
        api: "running",
      },
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      environment: process.env.NODE_ENV || "development",
    };

    res.status(dbHealthy ? 200 : 503).json(healthStatus);
  } catch (error) {
    res.status(503).json({
      status: "unhealthy",
      timestamp: new Date().toISOString(),
      error: error.message,
      services: {
        database: "error",
        api: "running",
      },
    });
  }
});

app.get("/api/metrics", async (req, res) => {
  try {
    const db = await ensureDbConnection();

    // Get basic metrics
    const accountCount = await db.collection("accounts").countDocuments();
    const jobCount = await db.collection("jobs").countDocuments();
    const syncHistoryCount = await db
      .collection("syncHistory")
      .countDocuments();

    // Get recent sync history
    const recentSyncs = await db
      .collection("syncHistory")
      .find({})
      .sort({ timestamp: -1 })
      .limit(10)
      .toArray();

    // Calculate success rate
    const successfulSyncs = recentSyncs.filter(
      (sync) => sync.status === "success"
    ).length;
    const successRate =
      recentSyncs.length > 0
        ? ((successfulSyncs / recentSyncs.length) * 100).toFixed(2)
        : 0;

    const metrics = {
      timestamp: new Date().toISOString(),
      counts: {
        accounts: accountCount,
        jobs: jobCount,
        syncHistory: syncHistoryCount,
      },
      recentSyncs: {
        total: recentSyncs.length,
        successful: successfulSyncs,
        failed: recentSyncs.length - successfulSyncs,
        successRate: `${successRate}%`,
      },
      system: {
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        nodeVersion: process.version,
        environment: process.env.NODE_ENV || "development",
      },
    };

    res.json(metrics);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

app.get("/api/cron/status", async (req, res) => {
  try {
    const db = await ensureDbConnection();

    // Get last cron job execution
    const lastCronSync = await db
      .collection("syncHistory")
      .find({ syncType: "jobs" })
      .sort({ timestamp: -1 })
      .limit(1)
      .toArray();

    // Get cron job statistics for the last 7 days
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);

    const recentCronSyncs = await db
      .collection("syncHistory")
      .find({
        syncType: "jobs",
        timestamp: { $gte: sevenDaysAgo },
      })
      .toArray();

    const successfulCronSyncs = recentCronSyncs.filter(
      (sync) => sync.status === "success"
    );
    const failedCronSyncs = recentCronSyncs.filter(
      (sync) => sync.status === "error"
    );

    const cronStatus = {
      timestamp: new Date().toISOString(),
      lastExecution:
        lastCronSync.length > 0
          ? {
              timestamp: lastCronSync[0].timestamp,
              status: lastCronSync[0].status,
              duration: lastCronSync[0].duration,
              details: lastCronSync[0].details,
            }
          : null,
      last7Days: {
        total: recentCronSyncs.length,
        successful: successfulCronSyncs.length,
        failed: failedCronSyncs.length,
        successRate:
          recentCronSyncs.length > 0
            ? `${(
                (successfulCronSyncs.length / recentCronSyncs.length) *
                100
              ).toFixed(2)}%`
            : "0%",
        averageDuration:
          successfulCronSyncs.length > 0
            ? successfulCronSyncs.reduce(
                (sum, sync) => sum + (sync.duration || 0),
                0
              ) / successfulCronSyncs.length
            : 0,
      },
      nextScheduled: {
        time: "09:00 UTC",
        frequency: "Daily",
        cronExpression: "0 9 * * *",
      },
    };

    res.json(cronStatus);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// Circuit breaker status endpoint
app.get("/api/circuit-breaker/status", (req, res) => {
  try {
    const status = {
      timestamp: new Date().toISOString(),
      workiz: workizCircuitBreaker.getState(),
      sheets: sheetsCircuitBreaker.getState(),
      summary: {
        workizState: workizCircuitBreaker.getState().state,
        sheetsState: sheetsCircuitBreaker.getState().state,
        workizFailures: workizCircuitBreaker.getState().failureCount,
        sheetsFailures: sheetsCircuitBreaker.getState().failureCount,
      },
    };

    res.json(status);
  } catch (error) {
    res.status(500).json({
      error: error.message,
      timestamp: new Date().toISOString(),
    });
  }
});

connectToMongoDB().then(() => {
  app.listen(port, () => {
    console.log(`Server running on port ${port}`);
  });
});

// Export for Vercel serverless functions
export default app;
