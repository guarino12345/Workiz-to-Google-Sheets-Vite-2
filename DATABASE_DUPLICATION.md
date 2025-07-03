# Database Duplication Guide

This guide will help you duplicate your current database into a new "workiz-sync" database. **Your application will continue using the original database** - this is just for creating a backup or separate copy.

## Prerequisites

- Your `MONGODB_URI` environment variable is set
- You have created the "workiz-sync" database in your MongoDB Atlas cluster
- You have the necessary permissions to read from the source database and write to the target database

## Step 1: Duplicate the Database

Run the duplication script to copy all data from your current database to the new "workiz-sync" database:

```bash
node duplicate-database.js
```

This script will:

- Connect to your current database (source)
- Connect to the new "workiz-sync" database (target)
- Copy all documents from the `accounts`, `jobs`, and `syncHistory` collections
- Verify that the duplication was successful
- Display document counts for verification

## Step 2: Verify the Duplication

Run the verification script to confirm that the new database contains all the expected data:

```bash
node update-database-config.js
```

This script will:

- Connect to the new "workiz-sync" database
- List all collections
- Count documents in each collection
- Display sample data from each collection
- Confirm the duplication was successful

## What This Accomplishes

✅ **Creates a complete copy** of your current database in "workiz-sync"  
✅ **Preserves your original database** - your app continues working unchanged  
✅ **Copies all collections**: accounts, jobs, syncHistory  
✅ **Verifies data integrity** with document counts and sample data  
✅ **Safe operation** - no changes to your application configuration

## Database Structure

After duplication, your "workiz-sync" database will contain:

- **`accounts`** - All your Workiz account configurations
- **`jobs`** - All synced job data from Workiz
- **`syncHistory`** - All sync operation history records

## Troubleshooting

### Common Issues:

1. **Connection Errors**: Ensure your `MONGODB_URI` is correct and includes proper authentication
2. **Permission Errors**: Verify that your MongoDB user has read/write permissions on both databases
3. **Missing Collections**: If collections are missing, check that the source database has the expected collections
4. **Data Mismatch**: If document counts don't match, check for any errors during the duplication process

### Verification Commands:

You can manually verify the duplication by running these MongoDB commands:

```javascript
// Connect to source database
use your-current-database
db.accounts.countDocuments()
db.jobs.countDocuments()
db.syncHistory.countDocuments()

// Connect to target database
use workiz-sync
db.accounts.countDocuments()
db.jobs.countDocuments()
db.syncHistory.countDocuments()
```

## Safety Notes

- The duplication script will **clear** the target collections before copying data
- If you want to append data instead of replacing it, comment out the `deleteMany()` line in the script
- Your original database and application remain completely unchanged
- The new database is independent and can be used for testing, backup, or other purposes

## Script Details

### `duplicate-database.js`

- Copies all documents from source to target collections
- Provides detailed logging of the process
- Verifies document counts after duplication
- Handles errors gracefully

### `update-database-config.js`

- Tests connectivity to the new database
- Verifies data integrity
- Shows sample data for verification
- Confirms successful duplication

## Next Steps (Optional)

After successful duplication, you can:

1. **Use the new database for testing** - safely test changes without affecting production
2. **Create additional backups** - run the script periodically to keep the copy updated
3. **Switch applications later** - if you decide to use the new database, you can update your app configuration separately
4. **Delete the copy** - if no longer needed, you can remove the "workiz-sync" database

**Important**: Your application will continue using the original database. This duplication is purely for creating a separate copy.
