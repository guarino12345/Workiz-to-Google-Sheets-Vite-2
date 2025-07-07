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
const workizCircuitBreaker = new CircuitBreaker(3, 600000); // 3 failures, 10 minutes recovery
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

    // Find account by ID - try both id and _id fields
    const account = await db.collection("accounts").findOne({
      $or: [{ _id: new ObjectId(accountId) }, { id: accountId }],
    });

    if (!account) {
      return res.status(404).json({ error: "Account not found" });
    }

    if (!account.workizApiToken) {
      return res
        .status(400)
        .json({ error: "Missing API token for this account" });
    }

    // Fetch jobs from Workiz API using the token from the account
    const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/all/?start_date=2025-01-01&offset=0&records=100&only_open=false`;

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

    if (!data.flag || !Array.isArray(data.data)) {
      return res
        .status(500)
        .json({ error: "Invalid response from Workiz API" });
    }

    // Filter jobs by sourceFilter if configured
    let filteredJobs = data.data;
    if (
      account.sourceFilter &&
      Array.isArray(account.sourceFilter) &&
      account.sourceFilter.length > 0
    ) {
      filteredJobs = data.data.filter((job) =>
        account.sourceFilter.includes(job.JobSource)
      );
    }

    if (filteredJobs.length === 0) {
      return res.json({
        message: `No jobs match the sourceFilter criteria for account ${
          account.name || "Unknown"
        }`,
        details: {
          jobsFromWorkiz: data.data.length,
          filteredJobs: 0,
          sourceFilter: account.sourceFilter,
        },
      });
    }

    // Add accountId to each filtered job
    const jobs = filteredJobs.map((job) => ({
      ...job,
      accountId: account._id || account.id,
    }));

    // Check for existing jobs with same UUIDs
    const existingJobCount = await db.collection("jobs").countDocuments({
      UUID: { $in: jobs.map((job) => job.UUID) },
    });

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
    }

    // Get all existing jobs for this account
    const existingJobs = await db
      .collection("jobs")
      .find({ accountId: account._id || account.id })
      .toArray();

    // Calculate 32-day cutoff date (standardized with cron job)
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - 32);

    // Clean up old jobs (older than 32 days)
    console.log(`🧹 Cleaning up old jobs (older than 32 days)...`);

    const deleteResult = await db.collection("jobs").deleteMany({
      accountId: account._id,
      JobDateTime: { $lt: cutoffDate.toISOString() },
    });

    console.log(
      `📊 Cleanup completed: ${deleteResult.deletedCount} old jobs deleted`
    );

    // Get final job count after sync and cleanup
    const finalJobCount = await db
      .collection("jobs")
      .countDocuments({ accountId: account._id || account.id });

    // Record sync history
    const syncHistoryRecord = {
      accountId: account._id || account.id,
      syncType: "jobs",
      status: "success",
      timestamp: new Date(),
      duration: Date.now() - accountStartTime,
      details: {
        jobsFromWorkiz: data.data.length,
        filteredJobs: jobs.length,
        existingJobsFound: existingJobs.length,
        finalJobCount: finalJobCount,
        jobsDeleted: deleteResult.deletedCount,
        syncMethod: "manual_standardized",
        sourceFilter: account.sourceFilter,
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
        jobsFromWorkiz: data.data.length,
        filteredJobs: jobs.length,
        existingJobsFound: existingJobs.length,
        finalJobCount: finalJobCount,
        jobsDeleted: deleteResult.deletedCount,
        sourceFilter: account.sourceFilter,
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
  const accountStartTime = Date.now();

  try {
    const { accountId } = req.params;
    console.log(`🕐 Manual sync triggered at: ${new Date().toISOString()}`);

    // Find account
    const account = await db.collection("accounts").findOne({
      $or: [{ _id: new ObjectId(accountId) }, { id: accountId }],
    });

    if (!account) {
      return res.status(404).json({ error: "Account not found" });
    }

    console.log(`📋 Processing account: ${account.name}`);

    // Sync jobs
    try {
      const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/all/?start_date=2025-01-01&offset=0&records=100&only_open=false`;
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

        // Batch processing (logging removed for cleaner output)

        // Process each job in the current batch
        for (const existingJob of batch) {
          try {
            const jobDate = new Date(existingJob.JobDateTime);

            // Check if job is older than 1 year
            if (jobDate < oneYearAgo) {
              await RetryHandler.withRetry(async () => {
                await db
                  .collection("jobs")
                  .deleteOne({ UUID: existingJob.UUID });
              });
              deletedJobsCount++;
              continue;
            }

            // Update job using Workiz API
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

                  // Check if response is HTML (520 error page)
                  if (
                    errorText.includes('<div class="text-container">') ||
                    errorText.includes("Oops!") ||
                    errorText.includes("Something went wrong")
                  ) {
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
              } else {
                // Job might have been deleted in Workiz, so delete from our database
                await RetryHandler.withRetry(async () => {
                  await db
                    .collection("jobs")
                    .deleteOne({ UUID: existingJob.UUID });
                });
                deletedJobsCount++;
              }
            } else {
              failedUpdatesCount++;
            }

            // Add a small delay between individual job updates (100ms)
            await new Promise((resolve) => setTimeout(resolve, 100));
          } catch (error) {
            failedUpdatesCount++;
          }
        }

        // Add delay between batches (except for the last batch)
        if (i + BATCH_SIZE < existingJobs.length) {
          await new Promise((resolve) =>
            setTimeout(resolve, DELAY_BETWEEN_BATCHES)
          );
        }
      }

      const accountDuration = Date.now() - accountStartTime;
      console.log(
        `${account.name}: ${existingJobs.length} jobs processed, ${updatedJobsCount} updated, ${failedUpdatesCount} failed, ${deletedJobsCount} deleted (${accountDuration}ms)`
      );

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
        `❌ Manual sync failed for account ${account.name}: ${error.message}`
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

// Cron job endpoint - Now uses parallel sync approach
app.get("/api/cron/sync-jobs", async (req, res) => {
  const startTime = Date.now();

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

    // Ensure database connection with health check
    const db = await ensureDbConnection();
    await DatabaseManager.ensureHealthyConnection(db);

    // Get all accounts with enhanced error handling
    const accounts = await RetryHandler.withRetry(async () => {
      const result = await db.collection("accounts").find({}).toArray();
      return result;
    });

    if (accounts.length === 0) {
      return res.json({
        message: "No accounts found to sync",
      });
    }

    // Create sync session for tracking (same as parallel sync)
    const syncSession = {
      sessionId: `cron_sync_${Date.now()}_${Math.random()
        .toString(36)
        .substr(2, 9)}`,
      startTime: new Date(),
      totalAccounts: accounts.length,
      accounts: accounts.map((account) => ({
        accountId: account._id,
        accountName: account.name,
        status: "pending",
        progress: 0,
        batches: [],
        totalJobs: 0,
        processedJobs: 0,
      })),
      overallStatus: "initializing",
      createdAt: new Date(),
      syncMethod: "cron",
    };

    // Store sync session
    await db.collection("syncSessions").insertOne(syncSession);

    // Process all accounts in parallel using the same logic as manual parallel sync
    const accountPromises = accounts.map(async (account) => {
      const accountStartTime = Date.now();

      try {
        // Update session status
        await db.collection("syncSessions").updateOne(
          {
            sessionId: syncSession.sessionId,
            "accounts.accountId": account._id,
          },
          {
            $set: {
              "accounts.$.status": "processing",
              "accounts.$.startTime": new Date(),
            },
          }
        );

        // Step 1: Fetch recent jobs from Workiz (14 days)
        const startDate = new Date();
        startDate.setDate(startDate.getDate() - 14);
        const formattedStartDate = startDate.toISOString().split("T")[0];

        const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiToken}/job/all/?start_date=${formattedStartDate}&offset=0&records=100&only_open=true`;

        const response = await RetryHandler.withRetry(
          () => APIManager.fetchWithTimeout(workizUrl, {}, 30000),
          3,
          2000,
          workizCircuitBreaker
        );

        if (!response.ok) {
          throw new Error(
            `Workiz API error: ${response.status} ${response.statusText}`
          );
        }

        const data = await response.json();

        if (!data.flag || !data.data) {
          throw new Error("Invalid response from Workiz API");
        }

        // Filter recent jobs by sourceFilter if configured
        let filteredRecentJobs = data.data;
        if (
          account.sourceFilter &&
          Array.isArray(account.sourceFilter) &&
          account.sourceFilter.length > 0
        ) {
          filteredRecentJobs = data.data.filter((job) =>
            account.sourceFilter.includes(job.JobSource)
          );
        }

        // Step 2: Get all existing jobs from database for this account
        const existingJobs = await db
          .collection("jobs")
          .find({ accountId: account._id })
          .toArray();

        // Step 3: Combine recent jobs with existing jobs (avoid duplicates)
        const recentJobUuids = new Set(
          filteredRecentJobs.map((job) => job.UUID)
        );
        const existingJobUuids = new Set(existingJobs.map((job) => job.UUID));

        // Add existing jobs that aren't in recent list
        const allJobsToUpdate = [...filteredRecentJobs];
        existingJobs.forEach((existingJob) => {
          if (!recentJobUuids.has(existingJob.UUID)) {
            allJobsToUpdate.push(existingJob);
          }
        });

        // Jobs to update calculated (logging removed for Vercel limit)

        // Add accountId to each job
        const jobs = allJobsToUpdate.map((job) => ({
          ...job,
          accountId: account._id,
        }));

        // Split jobs into batches (reduced for better rate limiting)
        const batchSize = 15;
        const batches = [];
        for (let i = 0; i < jobs.length; i += batchSize) {
          batches.push(jobs.slice(i, i + batchSize));
        }

        // Update session with batch info
        await db.collection("syncSessions").updateOne(
          {
            sessionId: syncSession.sessionId,
            "accounts.accountId": account._id,
          },
          {
            $set: {
              "accounts.$.totalJobs": jobs.length,
              "accounts.$.totalBatches": batches.length,
              "accounts.$.batches": batches.map((batch, index) => ({
                batchIndex: index,
                jobCount: batch.length,
                status: "pending",
                uuids: batch.map((job) => job.UUID),
              })),
            },
          }
        );

        // Process jobs in batches (simplified - no individual UUID updates)
        let processedJobs = 0;
        let updatedJobsCount = 0;
        let failedUpdatesCount = 0;

        for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
          const batch = batches[batchIndex];
          const batchStartTime = Date.now();

          // Update batch status
          await db.collection("syncSessions").updateOne(
            {
              sessionId: syncSession.sessionId,
              "accounts.accountId": account._id,
            },
            {
              $set: {
                "accounts.$[account].batches.$[batch].status": "processing",
                "accounts.$[account].batches.$[batch].startTime": new Date(),
              },
            },
            {
              arrayFilters: [
                { "account.accountId": account._id },
                { "batch.batchIndex": batchIndex },
              ],
            }
          );

          // Process each job in the batch (using data from Workiz list API)
          for (let jobIndex = 0; jobIndex < batch.length; jobIndex++) {
            const job = batch[jobIndex];

            try {
              // Add accountId and lastUpdated to job data
              const jobWithMetadata = {
                ...job,
                accountId: account._id,
                lastUpdated: new Date(),
              };

              // Update or insert job in database
              await db
                .collection("jobs")
                .updateOne(
                  { UUID: job.UUID, accountId: account._id },
                  { $set: jobWithMetadata },
                  { upsert: true }
                );

              updatedJobsCount++;
              processedJobs++;
            } catch (error) {
              failedUpdatesCount++;
              processedJobs++;
            }
          }

          const batchDuration = Date.now() - batchStartTime;

          // Update batch status
          await db.collection("syncSessions").updateOne(
            {
              sessionId: syncSession.sessionId,
              "accounts.accountId": account._id,
            },
            {
              $set: {
                "accounts.$[account].batches.$[batch].status": "completed",
                "accounts.$[account].batches.$[batch].endTime": new Date(),
                "accounts.$[account].batches.$[batch].duration": batchDuration,
                "accounts.$[account].batches.$[batch].processedJobs":
                  batch.length,
                "accounts.$[account].batches.$[batch].failedJobs":
                  batch.length - updatedJobsCount,
              },
            },
            {
              arrayFilters: [
                { "account.accountId": account._id },
                { "batch.batchIndex": batchIndex },
              ],
            }
          );

          // Update overall progress
          await db.collection("syncSessions").updateOne(
            {
              sessionId: syncSession.sessionId,
              "accounts.accountId": account._id,
            },
            {
              $set: {
                "accounts.$.processedJobs": processedJobs,
                "accounts.$.progress": Math.round(
                  (processedJobs / jobs.length) * 100
                ),
              },
            }
          );
        }

        const accountDuration = Date.now() - accountStartTime;

        // Update final account status
        await db.collection("syncSessions").updateOne(
          {
            sessionId: syncSession.sessionId,
            "accounts.accountId": account._id,
          },
          {
            $set: {
              "accounts.$.status": "completed",
              "accounts.$.endTime": new Date(),
              "accounts.$.duration": accountDuration,
              "accounts.$.processedJobs": processedJobs,
              "accounts.$.updatedJobs": updatedJobsCount,
              "accounts.$.failedJobs": failedUpdatesCount,
            },
          }
        );

        // Step 4: Clean up old jobs (older than 32 days)
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - 32);

        const deleteResult = await db.collection("jobs").deleteMany({
          accountId: account._id,
          CreatedDate: { $lt: cutoffDate.toISOString() },
        });

        // Record sync history
        const syncHistoryRecord = {
          accountId: account._id,
          syncType: "jobs",
          status: "success",
          timestamp: new Date(),
          duration: accountDuration,
          details: {
            jobsFromWorkiz: data.data.length,
            recentJobsFiltered: filteredRecentJobs.length,
            existingJobsInDb: existingJobs.length,
            totalJobsToUpdate: jobs.length,
            totalBatches: batches.length,
            batchSize: batchSize,
            jobsUpdated: updatedJobsCount,
            failedUpdates: failedUpdatesCount,
            jobsDeleted: deleteResult.deletedCount,
            syncMethod: "cron_comprehensive",
            sourceFilter: account.sourceFilter,
            jobStatusBreakdown: {
              submitted: jobs.filter((j) => j.Status === "Submitted").length,
              pending: jobs.filter((j) => j.Status === "Pending").length,
              completed: jobs.filter(
                (j) =>
                  j.Status === "Completed" ||
                  j.Status === "done pending approval"
              ).length,
              cancelled: jobs.filter((j) =>
                ["Cancelled", "Canceled", "Cancelled by Customer"].includes(
                  j.Status
                )
              ).length,
            },
          },
        };

        await db.collection("syncHistory").insertOne(syncHistoryRecord);

        // Account processing completed (logging removed for Vercel limit)

        return {
          account: account.name,
          success: true,
          duration: accountDuration,
          jobsSynced: jobs.length,
          jobsUpdated: updatedJobsCount,
          failedUpdates: failedUpdatesCount,
          jobsDeleted: deleteResult.deletedCount,
          recentJobs: filteredRecentJobs.length,
          existingJobs: existingJobs.length,
        };
      } catch (error) {
        console.error(`Error processing account ${account.name}:`, error);

        // Update account status to failed
        await db.collection("syncSessions").updateOne(
          {
            sessionId: syncSession.sessionId,
            "accounts.accountId": account._id,
          },
          {
            $set: {
              "accounts.$.status": "failed",
              "accounts.$.error": error.message,
              "accounts.$.endTime": new Date(),
            },
          }
        );

        return {
          account: account.name,
          success: false,
          duration: Date.now() - accountStartTime,
          error: error.message,
        };
      }
    });

    // Wait for all accounts to complete
    const results = await Promise.allSettled(accountPromises);

    // Process results
    const syncResults = results.map((result, index) => {
      if (result.status === "fulfilled") {
        return result.value;
      } else {
        return {
          account: accounts[index]?.name || `Account ${index}`,
          success: false,
          duration: 0,
          error: result.reason?.message || "Unknown error",
        };
      }
    });

    const totalDuration = Date.now() - startTime;
    const successfulSyncs = syncResults.filter((r) => r.success).length;
    const failedSyncs = syncResults.filter((r) => !r.success).length;

    // Update overall session status
    await db.collection("syncSessions").updateOne(
      { sessionId: syncSession.sessionId },
      {
        $set: {
          overallStatus: "completed",
          endTime: new Date(),
          duration: totalDuration,
          successfulAccounts: successfulSyncs,
          failedAccounts: failedSyncs,
        },
      }
    );

    // Cron sync completed (logging removed for Vercel limit)

    res.json({
      message: `Cron parallel sync completed: ${successfulSyncs} successful, ${failedSyncs} failed`,
      sessionId: syncSession.sessionId,
      duration: totalDuration,
      results: syncResults,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const duration = Date.now() - startTime;
    console.log(
      `❌ Cron parallel sync error after ${duration}ms: ${error.message}`
    );
    console.error("Full error:", error);

    res.status(500).json({
      error: error.message,
      duration: duration,
      timestamp: new Date().toISOString(),
    });
  }
});

// Cron job endpoint for Google Sheets sync - Now uses parallel processing
app.get("/api/cron/sync-sheets", async (req, res) => {
  const startTime = Date.now();

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
    console.log(`📊 Starting parallel Google Sheets sync process...`);

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
      `📋 Found ${accounts.length} accounts with Google Sheets ID for parallel processing`
    );

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

    // Process all accounts in parallel
    const accountPromises = accounts.map(async (account) => {
      const accountStartTime = Date.now();
      console.log(`Processing Google Sheets sync for account: ${account.name}`);

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
          return {
            account: account.name,
            success: true,
            duration: Date.now() - accountStartTime,
            jobsSynced: 0,
            message: "No jobs to sync",
          };
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

        return {
          account: account.name,
          success: true,
          duration: accountDuration,
          jobsSynced: filteredJobs.length,
          updatedRows: response.data.updates?.updatedRows || 0,
        };
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

        return {
          account: account.name,
          success: false,
          duration: accountDuration,
          error: error.message,
        };
      }
    });

    // Wait for all accounts to complete
    const results = await Promise.allSettled(accountPromises);

    // Process results
    const syncResults = results.map((result, index) => {
      if (result.status === "fulfilled") {
        return result.value;
      } else {
        return {
          account: accounts[index]?.name || `Account ${index}`,
          success: false,
          duration: 0,
          error: result.reason?.message || "Unknown error",
        };
      }
    });

    const totalDuration = Date.now() - startTime;
    const successfulSyncs = syncResults.filter((r) => r.success).length;
    const failedSyncs = syncResults.filter((r) => !r.success).length;

    console.log(
      `🎯 Google Sheets parallel cron job completed in ${totalDuration}ms:`
    );
    console.log(`   - Successful: ${successfulSyncs} accounts`);
    console.log(`   - Failed: ${failedSyncs} accounts`);

    res.json({
      message: `Google Sheets parallel cron job completed: ${successfulSyncs} successful, ${failedSyncs} failed`,
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

// ============================================================================
// PARALLEL ACCOUNT PROCESSING ENDPOINTS
// ============================================================================

// Initialize parallel sync for all accounts
app.post("/api/sync/parallel/init", async (req, res) => {
  const startTime = Date.now();
  // Parallel sync initialization (logging removed for Vercel limit)

  try {
    await ensureDbConnection();

    // Get all accounts
    const accounts = await db.collection("accounts").find({}).toArray();

    if (accounts.length === 0) {
      return res.status(404).json({
        error: "No accounts found",
        message: "Please create at least one account before starting sync",
      });
    }

    // Accounts found (logging removed for Vercel limit)

    // Create sync session for tracking
    const syncSession = {
      sessionId: `sync_${Date.now()}_${Math.random()
        .toString(36)
        .substr(2, 9)}`,
      startTime: new Date(),
      totalAccounts: accounts.length,
      accounts: accounts.map((account) => ({
        accountId: account._id,
        accountName: account.name,
        status: "pending",
        progress: 0,
        batches: [],
        totalJobs: 0,
        processedJobs: 0,
      })),
      overallStatus: "initializing",
      createdAt: new Date(),
    };

    // Store sync session
    await db.collection("syncSessions").insertOne(syncSession);

    // Return session info for client to start processing
    res.json({
      message: "Parallel sync initialized successfully",
      sessionId: syncSession.sessionId,
      totalAccounts: accounts.length,
      accounts: syncSession.accounts.map((acc) => ({
        accountId: acc.accountId,
        accountName: acc.accountName,
        status: acc.status,
      })),
      nextStep: "Start processing accounts in parallel",
    });
  } catch (error) {
    console.error("❌ Error initializing parallel sync:", error);
    res.status(500).json({
      error: "Failed to initialize parallel sync",
      message: error.message,
    });
  }
});

// Process a single account's jobs in batches
app.post("/api/sync/parallel/account/:accountId", async (req, res) => {
  const { accountId } = req.params;
  const { sessionId, batchSize = 29, delayMs = 2000 } = req.body;

  const accountStartTime = Date.now();

  try {
    await ensureDbConnection();

    // Get account details
    const account = await db.collection("accounts").findOne({
      _id: new ObjectId(accountId),
    });

    if (!account) {
      return res.status(404).json({
        error: "Account not found",
        accountId,
      });
    }

    // Processing account (logging removed for Vercel limit)

    // Update session status
    if (sessionId) {
      await db.collection("syncSessions").updateOne(
        { sessionId, "accounts.accountId": accountId },
        {
          $set: {
            "accounts.$.status": "processing",
            "accounts.$.startTime": new Date(),
          },
        }
      );
    }

    // Fetch jobs from Workiz
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 14);
    const formattedStartDate = startDate.toISOString().split("T")[0];

    const workizUrl = `https://api.workiz.com/api/v1/${account.workizToken}/job/all/?start_date=${formattedStartDate}&offset=0&records=100&only_open=true`;

    const response = await RetryHandler.withRetry(
      () => APIManager.fetchWithTimeout(workizUrl, {}, 30000),
      3,
      2000,
      workizCircuitBreaker
    );

    if (!response.ok) {
      throw new Error(
        `Workiz API error: ${response.status} ${response.statusText}`
      );
    }

    const data = await response.json();

    if (!data.flag || !data.data) {
      throw new Error("Invalid response from Workiz API");
    }

    // Filter jobs by sourceFilter if configured
    let filteredJobs = data.data;
    if (
      account.sourceFilter &&
      Array.isArray(account.sourceFilter) &&
      account.sourceFilter.length > 0
    ) {
      filteredJobs = data.data.filter((job) =>
        account.sourceFilter.includes(job.JobSource)
      );
      console.log(
        `Recent jobs filtered: ${data.data.length} → ${filteredJobs.length} jobs`
      );
    }

    // Add accountId to each job
    const jobs = filteredJobs.map((job) => ({
      ...job,
      accountId: account._id,
    }));

    // Split jobs into batches
    const batches = [];
    for (let i = 0; i < jobs.length; i += batchSize) {
      batches.push(jobs.slice(i, i + batchSize));
    }

    // Batches created (logging removed for cleaner output)

    // Update session with batch info
    if (sessionId) {
      await db.collection("syncSessions").updateOne(
        { sessionId, "accounts.accountId": accountId },
        {
          $set: {
            "accounts.$.totalJobs": jobs.length,
            "accounts.$.totalBatches": batches.length,
            "accounts.$.batches": batches.map((batch, index) => ({
              batchIndex: index,
              jobCount: batch.length,
              status: "pending",
              uuids: batch.map((job) => job.UUID),
            })),
          },
        }
      );
    }

    // Process batches with rate limiting
    let processedJobs = 0;
    let updatedJobsCount = 0;
    let failedUpdatesCount = 0;

    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      const batchStartTime = Date.now();

      console.log(
        `🔄 Processing batch ${batchIndex + 1}/${batches.length} (${
          batch.length
        } jobs)`
      );

      // Update batch status
      if (sessionId) {
        await db.collection("syncSessions").updateOne(
          {
            sessionId,
            "accounts.accountId": accountId,
          },
          {
            $set: {
              "accounts.$[account].batches.$[batch].status": "processing",
              "accounts.$[account].batches.$[batch].startTime": new Date(),
            },
          },
          {
            arrayFilters: [
              { "account.accountId": accountId },
              { "batch.batchIndex": batchIndex },
            ],
          }
        );
      }

      // Process each job in the batch (using data from Workiz list API)
      for (let jobIndex = 0; jobIndex < batch.length; jobIndex++) {
        const job = batch[jobIndex];

        try {
          // Add accountId and lastUpdated to job data
          const jobWithMetadata = {
            ...job,
            accountId: account._id,
            lastUpdated: new Date(),
          };

          // Update or insert job in database
          await db
            .collection("jobs")
            .updateOne(
              { UUID: job.UUID, accountId: account._id },
              { $set: jobWithMetadata },
              { upsert: true }
            );

          updatedJobsCount++;
          processedJobs++;
        } catch (error) {
          failedUpdatesCount++;
          processedJobs++;
        }
      }

      const batchDuration = Date.now() - batchStartTime;
      console.log(
        `Batch ${batchIndex + 1}/${batches.length}: ${
          batch.length
        } jobs, ${updatedJobsCount} updated, ${failedUpdatesCount} failed`
      );

      // Update batch status
      if (sessionId) {
        await db.collection("syncSessions").updateOne(
          {
            sessionId,
            "accounts.accountId": accountId,
          },
          {
            $set: {
              "accounts.$[account].batches.$[batch].status": "completed",
              "accounts.$[account].batches.$[batch].endTime": new Date(),
              "accounts.$[account].batches.$[batch].duration": batchDuration,
              "accounts.$[account].batches.$[batch].processedJobs":
                batch.length,
              "accounts.$[account].batches.$[batch].failedJobs":
                batch.length - updatedJobsCount,
            },
          },
          {
            arrayFilters: [
              { "account.accountId": accountId },
              { "batch.batchIndex": batchIndex },
            ],
          }
        );
      }

      // Update overall progress
      if (sessionId) {
        await db.collection("syncSessions").updateOne(
          { sessionId, "accounts.accountId": accountId },
          {
            $set: {
              "accounts.$.processedJobs": processedJobs,
              "accounts.$.progress": Math.round(
                (processedJobs / jobs.length) * 100
              ),
            },
          }
        );
      }
    }

    const accountDuration = Date.now() - accountStartTime;

    // Update final account status
    if (sessionId) {
      await db.collection("syncSessions").updateOne(
        { sessionId, "accounts.accountId": accountId },
        {
          $set: {
            "accounts.$.status": "completed",
            "accounts.$.endTime": new Date(),
            "accounts.$.duration": accountDuration,
            "accounts.$.processedJobs": processedJobs,
            "accounts.$.updatedJobs": updatedJobsCount,
            "accounts.$.failedJobs": failedUpdatesCount,
          },
        }
      );
    }

    // Record sync history
    const syncHistoryRecord = {
      accountId: account._id,
      syncType: "jobs",
      status: "success",
      timestamp: new Date(),
      duration: accountDuration,
      details: {
        jobsFromWorkiz: data.data.length,
        filteredJobs: jobs.length,
        totalBatches: batches.length,
        batchSize: batchSize,
        jobsUpdated: updatedJobsCount,
        failedUpdates: failedUpdatesCount,
        syncMethod: "parallel",
        sourceFilter: account.sourceFilter,
        jobStatusBreakdown: {
          submitted: jobs.filter((j) => j.Status === "Submitted").length,
          pending: jobs.filter((j) => j.Status === "Pending").length,
          completed: jobs.filter(
            (j) =>
              j.Status === "Completed" || j.Status === "done pending approval"
          ).length,
          cancelled: jobs.filter((j) =>
            ["Cancelled", "Canceled", "Cancelled by Customer"].includes(
              j.Status
            )
          ).length,
        },
      },
    };

    await db.collection("syncHistory").insertOne(syncHistoryRecord);

    console.log(
      `Account ${account.name} done: ${jobs.length} jobs, ${updatedJobsCount} updated, ${failedUpdatesCount} failed, ${accountDuration}ms`
    );

    res.json({
      message: `Successfully processed account ${account.name}`,
      accountId: account._id,
      accountName: account.name,
      duration: accountDuration,
      details: {
        jobsFromWorkiz: data.data.length,
        filteredJobs: jobs.length,
        totalBatches: batches.length,
        jobsUpdated: updatedJobsCount,
        failedUpdates: failedUpdatesCount,
        sourceFilter: account.sourceFilter,
      },
    });
  } catch (error) {
    console.error(`❌ Error processing account ${accountId}:`, error);

    // Update account status to failed
    if (sessionId) {
      await db.collection("syncSessions").updateOne(
        { sessionId, "accounts.accountId": accountId },
        {
          $set: {
            "accounts.$.status": "failed",
            "accounts.$.error": error.message,
            "accounts.$.endTime": new Date(),
          },
        }
      );
    }

    res.status(500).json({
      error: "Failed to process account",
      accountId,
      message: error.message,
    });
  }
});

// Get sync session status
app.get("/api/sync/parallel/status/:sessionId", async (req, res) => {
  const { sessionId } = req.params;

  try {
    await ensureDbConnection();

    const session = await db.collection("syncSessions").findOne({ sessionId });

    if (!session) {
      return res.status(404).json({
        error: "Sync session not found",
        sessionId,
      });
    }

    // Calculate overall progress
    const totalAccounts = session.accounts.length;
    const completedAccounts = session.accounts.filter(
      (acc) => acc.status === "completed"
    ).length;
    const failedAccounts = session.accounts.filter(
      (acc) => acc.status === "failed"
    ).length;
    const processingAccounts = session.accounts.filter(
      (acc) => acc.status === "processing"
    ).length;
    const pendingAccounts = session.accounts.filter(
      (acc) => acc.status === "pending"
    ).length;

    const overallProgress =
      totalAccounts > 0
        ? Math.round((completedAccounts / totalAccounts) * 100)
        : 0;

    // Determine overall status
    let overallStatus = "processing";
    if (completedAccounts === totalAccounts) {
      overallStatus = "completed";
    } else if (failedAccounts === totalAccounts) {
      overallStatus = "failed";
    } else if (completedAccounts + failedAccounts === totalAccounts) {
      overallStatus = "completed_with_errors";
    }

    res.json({
      sessionId: session.sessionId,
      startTime: session.startTime,
      overallStatus,
      overallProgress,
      totalAccounts,
      completedAccounts,
      failedAccounts,
      processingAccounts,
      pendingAccounts,
      accounts: session.accounts.map((acc) => ({
        accountId: acc.accountId,
        accountName: acc.accountName,
        status: acc.status,
        progress: acc.progress || 0,
        totalJobs: acc.totalJobs || 0,
        processedJobs: acc.processedJobs || 0,
        updatedJobs: acc.updatedJobs || 0,
        failedJobs: acc.failedJobs || 0,
        startTime: acc.startTime,
        endTime: acc.endTime,
        duration: acc.duration,
        error: acc.error,
      })),
    });
  } catch (error) {
    console.error("❌ Error getting sync status:", error);
    res.status(500).json({
      error: "Failed to get sync status",
      message: error.message,
    });
  }
});

// Get all sync sessions
app.get("/api/sync/parallel/sessions", async (req, res) => {
  try {
    await ensureDbConnection();

    const sessions = await db
      .collection("syncSessions")
      .find({})
      .sort({ createdAt: -1 })
      .limit(50)
      .toArray();

    res.json({
      sessions: sessions.map((session) => ({
        sessionId: session.sessionId,
        startTime: session.startTime,
        createdAt: session.createdAt,
        totalAccounts: session.totalAccounts,
        overallStatus: session.overallStatus,
        accounts: session.accounts.map((acc) => ({
          accountName: acc.accountName,
          status: acc.status,
          progress: acc.progress || 0,
        })),
      })),
    });
  } catch (error) {
    console.error("❌ Error getting sync sessions:", error);
    res.status(500).json({
      error: "Failed to get sync sessions",
      message: error.message,
    });
  }
});

// Cron job endpoint for updating jobs using UUID - runs at 2am UTC
app.get("/api/cron/update-jobs-uuid", async (req, res) => {
  const startTime = Date.now();

  try {
    // Enhanced security validation
    const userAgent = req.get("User-Agent");
    const clientIP = req.ip || req.connection.remoteAddress;

    console.log(`🕐 UUID Update Cron started at ${new Date().toISOString()}`);
    console.log(`📊 User-Agent: ${userAgent}`);
    console.log(`🌐 Client IP: ${clientIP}`);

    // Validate cron job request
    if (!userAgent || !userAgent.includes("Vercel")) {
      console.log(`❌ Invalid cron job request from: ${clientIP}`);
      return res.status(403).json({
        error: "Unauthorized cron job request",
        timestamp: new Date().toISOString(),
      });
    }

    await ensureDbConnection();

    // Get all active accounts
    const accounts = await db.collection("accounts").find({}).toArray();

    if (accounts.length === 0) {
      console.log(`⚠️ No accounts found for UUID update`);
      return res.json({
        message: "No accounts to update",
        timestamp: new Date().toISOString(),
      });
    }

    console.log(`📋 Found ${accounts.length} accounts for UUID update`);

    // Process each account
    const results = [];
    for (const account of accounts) {
      const accountStartTime = Date.now();
      console.log(`🔄 Processing account: ${account.name}`);

      try {
        // Get all jobs for this account
        const existingJobs = await db
          .collection("jobs")
          .find({ accountId: account._id })
          .toArray();

        if (existingJobs.length === 0) {
          console.log(`⚠️ No jobs found for account: ${account.name}`);
          results.push({
            account: account.name,
            success: true,
            jobsUpdated: 0,
            jobsDeleted: 0,
            failedUpdates: 0,
            duration: Date.now() - accountStartTime,
          });
          continue;
        }

        console.log(
          `📊 Found ${existingJobs.length} jobs to update for ${account.name}`
        );

        // Process jobs in batches
        const BATCH_SIZE = 10;
        const DELAY_BETWEEN_BATCHES = 15000; // 15 seconds between batches
        let updatedJobsCount = 0;
        let deletedJobsCount = 0;
        let failedUpdatesCount = 0;

        for (let i = 0; i < existingJobs.length; i += BATCH_SIZE) {
          const batch = existingJobs.slice(i, i + BATCH_SIZE);
          const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
          const totalBatches = Math.ceil(existingJobs.length / BATCH_SIZE);

          console.log(
            `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} jobs)`
          );

          for (const existingJob of batch) {
            try {
              // Update job using Workiz API
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

                    // Handle 429 rate limiting specifically
                    if (resp.status === 429) {
                      console.log(
                        `⚠️ Rate limit hit for job ${existingJob.UUID}, waiting 60 seconds...`
                      );
                      await new Promise((resolve) =>
                        setTimeout(resolve, 60000)
                      );
                      throw new Error(
                        `Rate limited: ${resp.status} ${resp.statusText}`
                      );
                    }

                    // Check if response is HTML (520 error page)
                    if (
                      errorText.includes('<div class="text-container">') ||
                      errorText.includes("Oops!") ||
                      errorText.includes("Something went wrong")
                    ) {
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
                2000,
                workizCircuitBreaker
              );

              if (updateResponse.ok) {
                const updateData = await updateResponse.json();

                if (updateData.flag && updateData.data) {
                  // Update the job with fresh data from Workiz
                  const updatedJob = {
                    ...updateData.data,
                    accountId: account._id,
                    lastUpdated: new Date(),
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
                } else {
                  // Job might have been deleted in Workiz, so delete from our database
                  await RetryHandler.withRetry(async () => {
                    await db
                      .collection("jobs")
                      .deleteOne({ UUID: existingJob.UUID });
                  });
                  deletedJobsCount++;
                }
              } else {
                failedUpdatesCount++;
              }

              // Rate limiting: 6-second delay between API calls (10 calls per minute)
              await new Promise((resolve) => setTimeout(resolve, 6000));
            } catch (error) {
              console.log(
                `❌ Failed to update job ${existingJob.UUID}: ${error.message}`
              );
              failedUpdatesCount++;
            }
          }

          // Add delay between batches (except for the last batch)
          if (i + BATCH_SIZE < existingJobs.length) {
            await new Promise((resolve) =>
              setTimeout(resolve, DELAY_BETWEEN_BATCHES)
            );
          }
        }

        const accountDuration = Date.now() - accountStartTime;

        console.log(
          `✅ Account ${account.name} completed: ${updatedJobsCount} updated, ${deletedJobsCount} deleted, ${failedUpdatesCount} failed`
        );

        // Record sync history
        const syncHistoryRecord = {
          accountId: account._id,
          syncType: "jobs_uuid_update",
          status: "success",
          timestamp: new Date(),
          duration: accountDuration,
          details: {
            totalJobs: existingJobs.length,
            jobsUpdated: updatedJobsCount,
            jobsDeleted: deletedJobsCount,
            failedUpdates: failedUpdatesCount,
            syncMethod: "cron_uuid_update",
          },
        };

        await RetryHandler.withRetry(async () => {
          await db.collection("syncHistory").insertOne(syncHistoryRecord);
        });

        results.push({
          account: account.name,
          success: true,
          jobsUpdated: updatedJobsCount,
          jobsDeleted: deletedJobsCount,
          failedUpdates: failedUpdatesCount,
          duration: accountDuration,
        });
      } catch (error) {
        console.error(`❌ Error processing account ${account.name}:`, error);
        results.push({
          account: account.name,
          success: false,
          error: error.message,
          duration: Date.now() - accountStartTime,
        });
      }
    }

    const totalDuration = Date.now() - startTime;
    const successfulUpdates = results.filter((r) => r.success).length;
    const failedUpdates = results.filter((r) => !r.success).length;

    console.log(
      `📊 UUID Update Cron completed: ${successfulUpdates} successful, ${failedUpdates} failed`
    );

    res.json({
      message: `UUID Update Cron completed: ${successfulUpdates} successful, ${failedUpdates} failed`,
      duration: totalDuration,
      results: results,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const duration = Date.now() - startTime;
    console.log(
      `❌ UUID Update Cron error after ${duration}ms: ${error.message}`
    );
    console.error("Full error:", error);

    res.status(500).json({
      error: error.message,
      duration: duration,
      timestamp: new Date().toISOString(),
    });
  }
});

// Master function for parallel account processing
app.post("/api/update-all-jobs-parallel", async (req, res) => {
  const startTime = Date.now();

  try {
    console.log(
      `🚀 Starting parallel job update for all accounts at ${new Date().toISOString()}`
    );

    await ensureDbConnection();

    // Get all active accounts
    const accounts = await db.collection("accounts").find({}).toArray();

    if (accounts.length === 0) {
      console.log(`⚠️ No accounts found for parallel update`);
      return res.json({
        message: "No accounts to update",
        timestamp: new Date().toISOString(),
      });
    }

    console.log(`📋 Found ${accounts.length} accounts for parallel processing`);

    // Create operation tracking record
    const operationId = new ObjectId().toString();
    const operationRecord = {
      operationId,
      status: "running",
      totalAccounts: accounts.length,
      completedAccounts: 0,
      failedAccounts: 0,
      startTime: new Date(),
      accounts: accounts.map((account) => ({
        accountId: account._id,
        accountName: account.name,
        status: "pending",
        jobsProcessed: 0,
        jobsUpdated: 0,
        jobsDeleted: 0,
        errors: [],
      })),
    };

    await db.collection("parallelOperations").insertOne(operationRecord);

    // Start parallel processing for each account
    const accountPromises = accounts.map(async (account) => {
      try {
        console.log(`🔄 Starting parallel update for account: ${account.name}`);

        // Update account status to running
        await db
          .collection("parallelOperations")
          .updateOne(
            { operationId, "accounts.accountId": account._id },
            { $set: { "accounts.$.status": "running" } }
          );

        // Call the account-specific worker function
        const workerResponse = await fetch(
          `${req.protocol}://${req.get("host")}/api/update-account-jobs/${
            account._id
          }`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "X-Operation-ID": operationId,
            },
          }
        );

        const workerResult = await workerResponse.json();

        // Update account status based on result
        const status = workerResponse.ok ? "completed" : "failed";
        await db.collection("parallelOperations").updateOne(
          { operationId, "accounts.accountId": account._id },
          {
            $set: {
              "accounts.$.status": status,
              "accounts.$.jobsProcessed": workerResult.jobsProcessed || 0,
              "accounts.$.jobsUpdated": workerResult.jobsUpdated || 0,
              "accounts.$.jobsDeleted": workerResult.jobsDeleted || 0,
              "accounts.$.errors": workerResult.errors || [],
            },
          }
        );

        // Update overall completion count
        const updateField = workerResponse.ok
          ? "completedAccounts"
          : "failedAccounts";
        await db
          .collection("parallelOperations")
          .updateOne({ operationId }, { $inc: { [updateField]: 1 } });

        console.log(
          `✅ Account ${account.name} ${status}: ${
            workerResult.jobsUpdated || 0
          } updated, ${workerResult.jobsDeleted || 0} deleted`
        );

        return {
          account: account.name,
          success: workerResponse.ok,
          ...workerResult,
        };
      } catch (error) {
        console.error(`❌ Error processing account ${account.name}:`, error);

        // Update account status to failed
        await db.collection("parallelOperations").updateOne(
          { operationId, "accounts.accountId": account._id },
          {
            $set: {
              "accounts.$.status": "failed",
              "accounts.$.errors": [error.message],
            },
          }
        );

        // Update failed count
        await db
          .collection("parallelOperations")
          .updateOne({ operationId }, { $inc: { failedAccounts: 1 } });

        return {
          account: account.name,
          success: false,
          error: error.message,
        };
      }
    });

    // Wait for all accounts to complete (with timeout)
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(
        () => reject(new Error("Parallel processing timeout")),
        11 * 60 * 1000
      ); // 11 minutes
    });

    const results = await Promise.race([
      Promise.all(accountPromises),
      timeoutPromise,
    ]);

    // Update final operation status
    const finalStatus = results.every((r) => r.success)
      ? "completed"
      : "completed_with_errors";
    await db.collection("parallelOperations").updateOne(
      { operationId },
      {
        $set: {
          status: finalStatus,
          endTime: new Date(),
          duration: Date.now() - startTime,
        },
      }
    );

    const totalDuration = Date.now() - startTime;
    const successfulUpdates = results.filter((r) => r.success).length;
    const failedUpdates = results.filter((r) => !r.success).length;

    console.log(
      `📊 Parallel update completed: ${successfulUpdates} successful, ${failedUpdates} failed`
    );

    res.json({
      operationId,
      message: `Parallel update completed: ${successfulUpdates} successful, ${failedUpdates} failed`,
      duration: totalDuration,
      results: results,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    const duration = Date.now() - startTime;
    console.log(
      `❌ Parallel update error after ${duration}ms: ${error.message}`
    );
    console.error("Full error:", error);

    res.status(500).json({
      error: error.message,
      duration: duration,
      timestamp: new Date().toISOString(),
    });
  }
});

// Worker function for individual account processing
app.post("/api/update-account-jobs/:accountId", async (req, res) => {
  const accountStartTime = Date.now();
  const { accountId } = req.params;
  const operationId = req.get("X-Operation-ID");

  try {
    console.log(`🔄 Starting account worker for account ID: ${accountId}`);

    await ensureDbConnection();

    // Find account by ID
    const account = await db.collection("accounts").findOne({
      $or: [{ _id: new ObjectId(accountId) }, { id: accountId }],
    });

    if (!account) {
      throw new Error("Account not found");
    }

    if (!account.workizApiToken) {
      throw new Error("Missing API token for this account");
    }

    console.log(`📊 Processing account: ${account.name}`);

    // Get all jobs for this account
    const existingJobs = await db
      .collection("jobs")
      .find({ accountId: account._id })
      .toArray();

    if (existingJobs.length === 0) {
      console.log(`⚠️ No jobs found for account: ${account.name}`);
      return res.json({
        account: account.name,
        success: true,
        jobsProcessed: 0,
        jobsUpdated: 0,
        jobsDeleted: 0,
        duration: Date.now() - accountStartTime,
      });
    }

    console.log(
      `📊 Found ${existingJobs.length} jobs to update for ${account.name}`
    );

    // Process jobs with rate limiting
    const BATCH_SIZE = 10;
    const DELAY_BETWEEN_BATCHES = 15000; // 15 seconds between batches
    let updatedJobsCount = 0;
    let deletedJobsCount = 0;
    let failedUpdatesCount = 0;
    const errors = [];

    // Calculate time budget (11 minutes to be safe)
    const TIME_BUDGET = 11 * 60 * 1000; // 11 minutes
    const startTime = Date.now();

    for (let i = 0; i < existingJobs.length; i += BATCH_SIZE) {
      // Check if we're running out of time
      if (Date.now() - startTime > TIME_BUDGET) {
        console.log(
          `⏰ Time budget exceeded for account ${account.name}, stopping at job ${i}`
        );
        break;
      }

      const batch = existingJobs.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(existingJobs.length / BATCH_SIZE);

      console.log(
        `📦 Processing batch ${batchNumber}/${totalBatches} (${batch.length} jobs) for ${account.name}`
      );

      for (const existingJob of batch) {
        try {
          // Update job using Workiz API
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

                // Handle 429 rate limiting specifically
                if (resp.status === 429) {
                  console.log(
                    `⚠️ Rate limit hit for job ${existingJob.UUID}, waiting 60 seconds...`
                  );
                  await new Promise((resolve) => setTimeout(resolve, 60000));
                  throw new Error(
                    `Rate limited: ${resp.status} ${resp.statusText}`
                  );
                }

                // Check if response is HTML (520 error page)
                if (
                  errorText.includes('<div class="text-container">') ||
                  errorText.includes("Oops!") ||
                  errorText.includes("Something went wrong")
                ) {
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
            2000,
            workizCircuitBreaker
          );

          if (updateResponse.ok) {
            const updateData = await updateResponse.json();

            if (updateData.flag && updateData.data) {
              // Update the job with fresh data from Workiz
              const updatedJob = {
                ...updateData.data,
                accountId: account._id,
                lastUpdated: new Date(),
              };

              await RetryHandler.withRetry(async () => {
                await db
                  .collection("jobs")
                  .updateOne({ UUID: existingJob.UUID }, { $set: updatedJob });
              });

              updatedJobsCount++;
            } else {
              // Job might have been deleted in Workiz, so delete from our database
              await RetryHandler.withRetry(async () => {
                await db
                  .collection("jobs")
                  .deleteOne({ UUID: existingJob.UUID });
              });
              deletedJobsCount++;
            }
          } else {
            failedUpdatesCount++;
          }

          // Rate limiting: 6-second delay between API calls (10 calls per minute)
          await new Promise((resolve) => setTimeout(resolve, 6000));
        } catch (error) {
          console.log(
            `❌ Failed to update job ${existingJob.UUID}: ${error.message}`
          );
          failedUpdatesCount++;
          errors.push(`Job ${existingJob.UUID}: ${error.message}`);
        }
      }

      // Add delay between batches (except for the last batch)
      if (i + BATCH_SIZE < existingJobs.length) {
        await new Promise((resolve) =>
          setTimeout(resolve, DELAY_BETWEEN_BATCHES)
        );
      }
    }

    const accountDuration = Date.now() - accountStartTime;

    console.log(
      `✅ Account ${account.name} completed: ${updatedJobsCount} updated, ${deletedJobsCount} deleted, ${failedUpdatesCount} failed`
    );

    // Record sync history
    const syncHistoryRecord = {
      accountId: account._id,
      syncType: "jobs_uuid_update",
      status: "success",
      timestamp: new Date(),
      duration: accountDuration,
      details: {
        totalJobs: existingJobs.length,
        jobsUpdated: updatedJobsCount,
        jobsDeleted: deletedJobsCount,
        failedUpdates: failedUpdatesCount,
        syncMethod: "parallel_worker",
        operationId: operationId,
      },
    };

    await RetryHandler.withRetry(async () => {
      await db.collection("syncHistory").insertOne(syncHistoryRecord);
    });

    res.json({
      account: account.name,
      success: true,
      jobsProcessed: existingJobs.length,
      jobsUpdated: updatedJobsCount,
      jobsDeleted: deletedJobsCount,
      failedUpdates: failedUpdatesCount,
      errors: errors,
      duration: accountDuration,
    });
  } catch (error) {
    const duration = Date.now() - accountStartTime;
    console.log(
      `❌ Account worker error after ${duration}ms: ${error.message}`
    );
    console.error("Full error:", error);

    res.status(500).json({
      account: accountId,
      success: false,
      error: error.message,
      duration: duration,
    });
  }
});

// Get parallel operation status
app.get("/api/parallel-operation/:operationId", async (req, res) => {
  try {
    const { operationId } = req.params;
    await ensureDbConnection();

    const operation = await db
      .collection("parallelOperations")
      .findOne({ operationId });

    if (!operation) {
      return res.status(404).json({ error: "Operation not found" });
    }

    res.json(operation);
  } catch (error) {
    console.error("Error fetching operation status:", error);
    res.status(500).json({ error: error.message });
  }
});
