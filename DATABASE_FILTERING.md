# Database Filtering Guide

This guide will help you filter your existing database to remove jobs that don't match your current source filter configuration.

## What This Does

The `filter-database.js` script will:

- **Read all accounts** from your database
- **Check each account's source filter** configuration
- **Find jobs** that don't match the source filter criteria
- **Remove those jobs** from the database
- **Provide detailed statistics** about the filtering process

## Prerequisites

- Your `MONGODB_URI` environment variable is set
- You have configured source filters for your accounts in the app
- You want to clean up existing data to match your current filter settings

## Step 1: Review Your Source Filters

Before running the script, make sure your source filters are configured correctly:

1. **Open your app** and go to the Accounts section
2. **Check each account's source filter** (e.g., "Google", "GMB")
3. **Verify the filters** match what you want to keep

## Step 2: Run the Filtering Script

Execute the database filtering script:

```bash
node filter-database.js
```

## What the Script Does

### For Each Account:

1. **Reads the source filter** configuration
2. **Finds all jobs** for that account
3. **Identifies jobs to remove** (those not matching the filter)
4. **Shows a preview** of jobs being removed
5. **Removes the jobs** from the database
6. **Reports statistics** for that account

### Safety Features:

- **Shows preview** of jobs being removed before deletion
- **Keeps jobs with no JobSource** (doesn't remove them)
- **Provides detailed logging** of the process
- **Reports final statistics** and database size reduction

## Example Output

```
🔍 Starting database filtering process...
📊 Connected to database: workiz-sync
📋 Found 2 accounts

🔍 Processing account: Main Account
📋 Source filter: ["Google", "GMB"]
📊 Found 800 jobs for this account
🔍 Jobs to keep: 150
🗑️ Jobs to remove: 650
📋 Sample jobs being removed:
   - ABC123: Facebook (John Doe)
   - DEF456: Yelp (Jane Smith)
   - GHI789: Direct (Bob Johnson)
   ... and 647 more
✅ Removed 650 jobs from database
📈 Final job count for Main Account: 150 jobs

🎯 Database filtering completed!
📊 Summary:
   - Total jobs before: 800
   - Total jobs after: 150
   - Total jobs removed: 650
   - Database size reduction: 81.3%
```

## Benefits

✅ **Reduced Database Size**: Smaller, more focused dataset  
✅ **Better Performance**: Faster queries and sync operations  
✅ **Consistent Data**: Only relevant jobs for conversion tracking  
✅ **Vercel Compatibility**: Reduced processing time for sync operations  
✅ **Clean Data**: Removes irrelevant job sources

## Safety Notes

- **Backup First**: Consider backing up your database before running this script
- **Review Output**: Check the preview of jobs being removed
- **Test First**: Run on a test database if possible
- **Irreversible**: This operation permanently removes jobs from the database

## Troubleshooting

### Common Issues:

1. **No accounts found**: Make sure you have accounts configured in your app
2. **No source filters**: Accounts without source filters will be skipped
3. **Connection errors**: Verify your `MONGODB_URI` is correct
4. **Permission errors**: Ensure your MongoDB user has delete permissions

### Verification Commands:

After running the script, you can verify the results:

```javascript
// Connect to your database
use workiz-sync

// Check total job count
db.jobs.countDocuments()

// Check jobs by account
db.jobs.countDocuments({ accountId: "your-account-id" })

// Check jobs by source
db.jobs.countDocuments({ JobSource: "Google" })
```

## Next Steps

After filtering your database:

1. **Test your sync operations** to ensure they work correctly
2. **Monitor sync performance** - should be faster now
3. **Check your Google Sheets sync** - should only include filtered jobs
4. **Verify conversion tracking** is working as expected

## Script Details

### `filter-database.js`

- **Safe filtering**: Only removes jobs that explicitly don't match filters
- **Detailed logging**: Shows exactly what's being removed
- **Statistics reporting**: Provides comprehensive summary
- **Error handling**: Graceful error handling and cleanup
- **Progress tracking**: Shows progress for each account

The script is designed to be safe and informative, giving you full visibility into what data is being removed before making any changes.
