# Batch Processing Optimization for Vercel

This document outlines the optimizations made to the batch processing system to handle large-scale job updates efficiently on Vercel without hitting function timeout limits.

## Overview

The batch processing system implements several key strategies recommended by Vercel for handling large data operations:

1. **Background Processing**: Uses fire-and-forget HTTP calls to trigger batch processing
2. **Incremental Updates**: Jobs are split into manageable batches (10 jobs per batch)
3. **Progress Tracking**: Real-time progress monitoring via dedicated endpoints
4. **Error Recovery**: Retry logic for failed batches with exponential backoff
5. **Database Optimization**: Strategic indexing for better query performance

## Architecture

### 1. Batch Lifecycle Management

```
Initialize Operation → Create Batches → Process Sequentially → Track Progress → Handle Errors
```

- **Operation Initialization**: `/api/init-batch-update` - Creates batches for all accounts
- **Batch Processing**: `/api/process-batch/:batchId` - Processes individual batches
- **Progress Tracking**: `/api/batch-progress/:operationId` - Monitors overall progress
- **Error Recovery**: `/api/retry-batch/:batchId` - Retries failed batches

### 2. Database Schema

#### Batches Collection

```javascript
{
  _id: ObjectId,
  operationId: String,
  accountId: ObjectId,
  batchNumber: Number,
  status: "pending" | "processing" | "completed" | "failed" | "completed_with_errors",
  jobUUIDs: [String],
  completedJobs: [String],
  failedJobs: [String],
  errors: [String],
  retryCount: Number,
  startTime: Date,
  endTime: Date
}
```

#### Batch Account States Collection

```javascript
{
  _id: ObjectId,
  operationId: String,
  accountId: ObjectId,
  currentBatch: Number,
  totalBatches: Number,
  status: "processing" | "completed",
  lastBatchCompleted: Number,
  nextBatchToProcess: Number,
  startTime: Date,
  endTime: Date
}
```

## Key Optimizations

### 1. Vercel Configuration

**vercel.json** includes function timeout configuration:

```json
{
  "functions": {
    "server.js": {
      "maxDuration": 60
    }
  }
}
```

This extends the function timeout to 60 seconds for batch processing operations.

### 2. Database Indexes

Strategic indexes for optimal query performance:

```javascript
// Jobs collection
{ UUID: 1, accountId: 1 } // Unique compound index
{ accountId: 1 } // Account-based queries
{ lastUpdated: 1 } // Time-based queries
{ JobDateTime: 1 } // Job scheduling queries

// Batches collection
{ operationId: 1 } // Operation-based queries
{ accountId: 1 } // Account-based queries
{ status: 1 } // Status-based queries
{ operationId: 1, accountId: 1, status: 1 } // Compound index for batch processing

// Batch Account States collection
{ operationId: 1 } // Operation-based queries
{ accountId: 1 } // Account-based queries
{ operationId: 1, accountId: 1 } // Compound index for account state tracking
```

### 3. Real Workiz API Integration

The batch processing now includes real Workiz API calls:

```javascript
// Fetch job details from Workiz API
const workizUrl = `https://api.workiz.com/api/v1/${account.workizApiKey}/job/get/${uuid}/`;
const response = await fetch(workizUrl);

if (jobData.flag && jobData.data) {
  // Update job in database with real data
  await db.collection("jobs").updateOne(
    { UUID: uuid, accountId: batch.accountId },
    {
      $set: {
        ...jobData.data,
        lastUpdated: new Date(),
        accountId: batch.accountId,
      },
    },
    { upsert: true }
  );
}
```

### 4. Error Handling and Retry Logic

- **Retry Count**: Each batch tracks retry attempts (max 3)
- **Status Tracking**: Batches can be "completed", "failed", or "completed_with_errors"
- **Error Recovery**: Failed batches can be retried via `/api/retry-batch/:batchId`
- **Rate Limiting**: 1-second delay between API calls to respect Workiz rate limits

### 5. Background Processing

Uses fire-and-forget HTTP calls to trigger batch processing:

```javascript
// Trigger next batch asynchronously
setTimeout(async () => {
  try {
    await fetch(`${baseUrl}/api/process-batch/${nextBatch._id}`, {
      method: "POST",
    });
  } catch (error) {
    console.error(`Failed to trigger next batch ${nextBatch._id}:`, error);
  }
}, 1000);
```

## Usage

### 1. Initialize Batch Update

```bash
POST /api/init-batch-update
```

Creates batches for all accounts and starts processing.

### 2. Monitor Progress

```bash
GET /api/batch-progress/:operationId
```

Returns real-time progress information for the operation.

### 3. Retry Failed Batch

```bash
POST /api/retry-batch/:batchId
```

Retries a failed batch (up to 3 attempts).

## Performance Characteristics

- **Batch Size**: 10 jobs per batch (configurable)
- **Processing Rate**: 1 job per second (respects Workiz rate limits)
- **Concurrency**: Sequential per account, parallel across accounts
- **Timeout**: 60 seconds per function invocation
- **Retry Logic**: Up to 3 attempts per batch
- **Error Recovery**: Manual retry via API endpoint

## Monitoring and Debugging

### Logs to Monitor

1. **Batch Processing**: `Failed to update job ${uuid}:`
2. **API Errors**: `Workiz API error: ${response.status}`
3. **Trigger Failures**: `Failed to trigger next batch ${batchId}:`

### Key Metrics

- **Completion Rate**: Percentage of successfully processed jobs
- **Error Rate**: Percentage of failed jobs
- **Processing Time**: Time per batch and total operation time
- **Retry Count**: Number of retries per batch

## Best Practices

1. **Monitor Progress**: Use the progress endpoint to track operation status
2. **Handle Errors**: Check for failed batches and retry if necessary
3. **Rate Limiting**: Respect Workiz API rate limits (1 request per second)
4. **Database Health**: Ensure MongoDB indexes are created for optimal performance
5. **Error Recovery**: Use the retry endpoint for failed batches

## Troubleshooting

### Common Issues

1. **Timeout Errors**: Check if batches are too large or processing too slowly
2. **API Errors**: Verify Workiz API keys and rate limits
3. **Database Errors**: Ensure MongoDB connection and indexes are properly configured
4. **Concurrency Issues**: Check for multiple batches processing simultaneously for the same account

### Solutions

1. **Reduce Batch Size**: Decrease BATCH_SIZE if timeouts occur
2. **Increase Delays**: Add more delay between API calls if rate limited
3. **Check Logs**: Monitor server logs for specific error messages
4. **Retry Failed Batches**: Use the retry endpoint for failed operations
