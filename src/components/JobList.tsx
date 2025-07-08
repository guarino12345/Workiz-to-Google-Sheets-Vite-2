import React, { useState, useEffect } from 'react';
import {
  Box,
  Typography,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  Alert,
  AlertTitle,
  Button,
  Link,
  LinearProgress,
  Card,
  CardContent,
  Chip,
  Collapse,
  IconButton,
} from '@mui/material';
import { 
  CheckCircle, 
  Error as ErrorIcon, 
  ExpandMore, 
  ExpandLess,
  CloudDownload,
  CloudUpload,
  Refresh,
  Update
} from '@mui/icons-material';
import { Account } from '../types/index';
import { buildApiUrl } from '../utils/api';

interface Job {
  UUID: string;
  FirstName: string;
  LastName: string;
  Address: string;
  City: string;
  State: string;
  JobType: string;
  JobSource: string;
  Status: string;
  JobDateTime: string;
  JobTotalPrice: number;
  apiKey?: string;
  accountId: string;
}

interface SyncProgress {
  phase: 'fetching' | 'processing' | 'updating' | 'cleaning' | 'complete';
  percentage: number;
  message: string;
  details?: string;
}

interface SyncResult {
  success: boolean;
  message: string;
  details?: {
    jobsFromWorkiz?: number;
    existingJobsFound?: number;
    finalJobCount?: number;
    jobsUpdated?: number;
    jobsDeleted?: number;
    failedUpdates?: number;
    successfulAccounts?: number;
    totalAccounts?: number;
    failedAccounts?: number;
    totalBatches?: number;
    completedBatches?: number;
    failedBatches?: number;
    operationId?: string;
    resumeInfo?: any;
  };
  timestamp: Date;
}

interface JobListProps {
  accounts: Account[];
}

// Helper function to safely extract error messages
function getErrorMessage(err: unknown): string {
  if (err instanceof Error) {
    return (err as Error).message;
  }
  if (typeof err === 'string') {
    return err;
  }
  return 'An unknown error occurred';
}

const JobList: React.FC<JobListProps> = ({ accounts }) => {
  const [selectedAccount, setSelectedAccount] = useState<Account | null>(null);
  const [allJobs, setAllJobs] = useState<Job[]>([]); // Store all jobs for counting
  const [error, setError] = useState<string>('');
  const [syncing, setSyncing] = useState(false);
  const [syncingToSheets, setSyncingToSheets] = useState(false);
  const [updatingJobs, setUpdatingJobs] = useState(false);
  const [currentOperationId, setCurrentOperationId] = useState<string | null>(null);
  const [operationProgress, setOperationProgress] = useState<any>(null);
  
  // New state for enhanced loading and progress
  const [syncProgress, setSyncProgress] = useState<SyncProgress | null>(null);
  const [syncResult, setSyncResult] = useState<SyncResult | null>(null);
  const [showSyncDetails, setShowSyncDetails] = useState(false);
  
  // Resume functionality state
  const [resumeInfo, setResumeInfo] = useState<any>(null);
  const [resuming, setResuming] = useState(false);

  // Set initial selected account only once when component mounts
  useEffect(() => {
    if (accounts.length > 0 && !selectedAccount) {
      setSelectedAccount(accounts[0]);
    }
  }, []);

  // Fetch ALL jobs from DB when component mounts or accounts change
  useEffect(() => {
    const fetchAllJobs = async () => {
      if (accounts.length === 0) return;
      try {
        const response = await fetch(buildApiUrl('/api/jobs'));
        if (!response.ok) {
          console.error('Failed to fetch jobs from DB');
          setAllJobs([]);
          return;
        }
        const data = await response.json();
        setAllJobs(data);
      } catch (err) {
        console.error('Failed to fetch all jobs:', err);
        setAllJobs([]);
      }
    };
    fetchAllJobs();
  }, [accounts, syncing, updatingJobs]);

  // Poll operation progress when operation is running
  useEffect(() => {
    if (!currentOperationId) return;

    const pollInterval = setInterval(async () => {
      try {
        const response = await fetch(buildApiUrl(`/api/parallel-operation/${currentOperationId}`));
        if (response.ok) {
          const operation = await response.json();
          setOperationProgress(operation);
          
          // Stop polling if operation is complete
          if (operation.status === 'completed' || operation.status === 'completed_with_errors') {
            setCurrentOperationId(null);
            clearInterval(pollInterval);
          }
        }
      } catch (error) {
        console.error('Error polling operation status:', error);
      }
    }, 2000); // Poll every 2 seconds

    return () => clearInterval(pollInterval);
  }, [currentOperationId]);

  const handleAccountChange = (accountId: string) => {
    if (!accountId) {
      setSelectedAccount(null);
      return;
    }
    
    const account = accounts.find(a => a.id === accountId);
    if (account) {
      console.log('Manually selected account:', account.name);
      setSelectedAccount(account);
    } else {
      console.error('Account not found:', accountId);
      setSelectedAccount(null);
    }
  };

  const updateSyncProgress = (phase: SyncProgress['phase'], percentage: number, message: string, details?: string) => {
    setSyncProgress({
      phase,
      percentage,
      message,
      details
    });
  };

  // Sync Latest Jobs - fetches new jobs from Workiz
  const handleSyncLatestJobs = async () => {
    if (!selectedAccount?.id) {
      console.error('No account ID available for sync');
      return;
    }
    
    setSyncing(true);
    setError('');
    setSyncResult(null);
    setShowSyncDetails(false);
    
    // Initialize progress
    updateSyncProgress('fetching', 0, 'Initializing sync...');
    
    try {
      console.log('Syncing latest jobs for account:', selectedAccount.id);
      
      // Simulate progress updates for better UX
      updateSyncProgress('fetching', 10, 'Connecting to Workiz API...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      updateSyncProgress('fetching', 25, 'Fetching latest jobs from Workiz...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      updateSyncProgress('processing', 40, 'Processing job data...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      const response = await fetch(
        buildApiUrl(`/api/sync-jobs/${selectedAccount.id}`),
        { method: 'POST' }
      );
      
      updateSyncProgress('updating', 70, 'Updating database...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      if (!response.ok) {
        const errorData = await response.json() as { error?: string };
        const errorMessage = errorData.error || 'Failed to sync latest jobs';
        setError(errorMessage);
        setSyncResult({
          success: false,
          message: 'Sync failed',
          timestamp: new Date()
        });
        return;
      }
      
      const result = await response.json() as { details?: any };
      
      updateSyncProgress('complete', 100, 'Sync completed successfully!');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      setSyncResult({
        success: true,
        message: `Successfully synced latest jobs`,
        details: result.details,
        timestamp: new Date()
      });
      
      console.log('Sync successful:', result);
    } catch (err) {
      console.error('Sync error:', err);
      setError(getErrorMessage(err));
      setSyncResult({
        success: false,
        message: 'Sync failed',
        timestamp: new Date()
      });
    } finally {
      setSyncing(false);
      setSyncProgress(null);
    }
  };

  // Update All Jobs - updates existing jobs by UUID using batch lifecycle management
  const handleUpdateAllJobs = async () => {
    setUpdatingJobs(true);
    setError('');
    setSyncResult(null);
    setShowSyncDetails(false);
    
    // Initialize progress
    updateSyncProgress('fetching', 0, 'Initializing batch job updates...');
    
    try {
      console.log('Starting batch job updates for all accounts');
      
      updateSyncProgress('fetching', 10, 'Preparing batch processing...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      updateSyncProgress('fetching', 25, 'Creating job batches...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      // Call the batch initialization endpoint
      const response = await fetch(
        buildApiUrl(`/api/initiate-batch-update`),
        { 
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );
      
      if (!response.ok) {
        const errorData = await response.json() as { error?: string };
        const errorMessage = errorData.error || 'Failed to initiate batch updates';
        setError(errorMessage);
        setSyncResult({
          success: false,
          message: 'Batch initiation failed',
          timestamp: new Date()
        });
        return;
      }
      
      const result = await response.json() as { 
        operationId?: string; 
        accounts?: number;
        batches?: number;
      };
      
      console.log('Batch initialization completed:', result);
      
      if (result.operationId) {
        setCurrentOperationId(result.operationId);
        updateSyncProgress('processing', 40, 'Batches are being processed. Monitoring progress...');
        
        // Start polling for batch progress
        await pollBatchProgress(result.operationId);
      } else {
        throw new Error('No operation ID received from server');
      }
      
    } catch (err) {
      console.error('Update error:', err);
      setError(getErrorMessage(err));
      setSyncResult({
        success: false,
        message: 'Update failed',
        timestamp: new Date()
      });
    } finally {
      setUpdatingJobs(false);
      setSyncProgress(null);
    }
  };

  // Poll progress for batch operations
  const pollBatchProgress = async (operationId: string) => {
    const maxAttempts = 120; // 10 minutes with 5-second intervals
    let attempts = 0;
    
    const poll = async (): Promise<void> => {
      try {
        const response = await fetch(buildApiUrl(`/api/batch-progress/${operationId}`));
        
        if (!response.ok) {
          throw new Error('Failed to fetch batch progress');
        }
        
        const progress = await response.json() as {
          operationId: string;
          accounts: Array<{
            accountId: string;
            currentBatch: number;
            totalBatches: number;
            status: string;
            lastBatchCompleted: number;
            nextBatchToProcess: number | null;
            startTime: string;
            endTime: string | null;
          }>;
          batches: Array<{
            _id: string;
            accountId: string;
            batchNumber: number;
            status: string;
            jobUUIDs: string[];
            completedJobs: string[];
            failedJobs: string[];
            startTime: string | null;
            endTime: string | null;
            errors: string[];
          }>;
        };
        
        // Calculate overall progress
        const totalAccounts = progress.accounts.length;
        const completedAccounts = progress.accounts.filter(acc => acc.status === 'completed').length;
        const processingAccounts = progress.accounts.filter(acc => acc.status === 'processing').length;
        const failedAccounts = progress.accounts.filter(acc => acc.status === 'failed').length;
        
        const totalBatches = progress.batches.length;
        const completedBatches = progress.batches.filter(batch => batch.status === 'completed').length;
        const processingBatches = progress.batches.filter(batch => batch.status === 'processing').length;
        const failedBatches = progress.batches.filter(batch => batch.status === 'failed').length;
        
        const totalJobs = progress.batches.reduce((sum, batch) => sum + batch.jobUUIDs.length, 0);
        const completedJobs = progress.batches.reduce((sum, batch) => sum + batch.completedJobs.length, 0);
        const failedJobs = progress.batches.reduce((sum, batch) => sum + batch.failedJobs.length, 0);
        
        // Calculate completion percentage
        const accountPercentage = totalAccounts > 0 ? (completedAccounts / totalAccounts) * 100 : 0;
        const batchPercentage = totalBatches > 0 ? (completedBatches / totalBatches) * 100 : 0;
        const overallPercentage = Math.round((accountPercentage + batchPercentage) / 2);
        
        // Determine phase
        const isComplete = completedAccounts === totalAccounts;
        const hasErrors = failedAccounts > 0 || failedBatches > 0;
        const phase = isComplete ? 'complete' : 'processing';
        
        // Update progress display
        updateSyncProgress(
          phase, 
          overallPercentage, 
          `Accounts: ${completedAccounts}/${totalAccounts} completed | Batches: ${completedBatches}/${totalBatches} completed`,
          `Jobs: ${completedJobs} completed, ${failedJobs} failed`
        );
        
        // Check if operation is complete
        if (isComplete) {
          setSyncResult({
            success: !hasErrors,
            message: hasErrors 
              ? `Completed with errors: ${failedAccounts} accounts, ${failedBatches} batches failed`
              : `Successfully completed all batches across ${totalAccounts} accounts`,
            details: {
              jobsUpdated: completedJobs,
              jobsDeleted: 0, // Not tracked in batch system
              successfulAccounts: completedAccounts,
              totalAccounts: totalAccounts,
              failedAccounts: failedAccounts,
              totalBatches: totalBatches,
              completedBatches: completedBatches,
              failedBatches: failedBatches,
              operationId: operationId
            },
            timestamp: new Date()
          });
          
          return; // Stop polling
        }
        
        // Continue polling if not complete
        attempts++;
        if (attempts >= maxAttempts) {
          throw new Error('Batch progress polling timeout - operation may still be running');
        }
        
        // Wait 5 seconds before next poll
        await new Promise(resolve => setTimeout(resolve, 5000));
        return poll();
        
      } catch (error) {
        console.error('Batch progress polling error:', error);
        throw error;
      }
    };
    
    return poll();
  };

  // Resume update for a specific account
  const handleResumeUpdate = async (accountId: string, resumeFrom: string) => {
    setResuming(true);
    setError('');
    setSyncResult(null);
    setShowSyncDetails(false);
    
    // Initialize progress
    updateSyncProgress('fetching', 0, 'Resuming job updates...');
    
    try {
      console.log('Resuming job updates for account:', accountId, 'from job:', resumeFrom);
      
      updateSyncProgress('fetching', 25, 'Connecting to account...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      updateSyncProgress('processing', 50, 'Processing remaining jobs...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      const response = await fetch(
        buildApiUrl(`/api/resume-account-update/${accountId}`),
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ resumeFrom })
        }
      );
      
      updateSyncProgress('updating', 75, 'Updating jobs in database...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      if (!response.ok) {
        const errorData = await response.json() as { error?: string };
        const errorMessage = errorData.error || 'Failed to resume job updates';
        setError(errorMessage);
        setSyncResult({
          success: false,
          message: 'Resume failed',
          timestamp: new Date()
        });
        return;
      }
      
      const result = await response.json();
      
      updateSyncProgress('complete', 100, 'Resume completed successfully!');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      setSyncResult({
        success: true,
        message: `Successfully resumed and completed job updates`,
        details: {
          jobsUpdated: result.jobsUpdated || 0,
          jobsDeleted: result.jobsDeleted || 0,
          failedUpdates: result.failedUpdates || 0,
          resumeInfo: result.resumeInfo
        },
        timestamp: new Date()
      });
      
      // Clear resume info if all jobs are processed
      if (result.resumeInfo && !result.resumeInfo.canResume) {
        setResumeInfo(null);
      }
      
      console.log('Resume successful:', result);
    } catch (err) {
      console.error('Resume error:', err);
      setError(getErrorMessage(err));
      setSyncResult({
        success: false,
        message: 'Resume failed',
        timestamp: new Date()
      });
    } finally {
      setResuming(false);
      setSyncProgress(null);
    }
  };

  // Sync to Google Sheets
  const handleSyncToSheets = async () => {
    if (!selectedAccount?.id) {
      console.error('No account ID available for Google Sheets sync');
      return;
    }
    
    setSyncingToSheets(true);
    setError('');
    setSyncResult(null);
    setShowSyncDetails(false);
    
    // Initialize progress
    updateSyncProgress('fetching', 20, 'Preparing Google Sheets sync...');
    
    try {
      console.log('Syncing to Google Sheets for account:', selectedAccount.id);
      
      updateSyncProgress('processing', 50, 'Syncing to Google Sheets...');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      const response = await fetch(
        buildApiUrl(`/api/sync-to-sheets/${selectedAccount.id}`),
        { method: 'POST' }
      );
      
      if (!response.ok) {
        const data = await response.json() as { error?: string };
        const errorMessage = data.error || 'Failed to sync to Google Sheets';
        setError(errorMessage);
        setSyncResult({
          success: false,
          message: 'Google Sheets sync failed',
          timestamp: new Date()
        });
        return;
      }
      
      const data = await response.json();
      
      updateSyncProgress('complete', 100, 'Google Sheets sync completed!');
      await new Promise<void>(resolve => setTimeout(resolve, 500));
      
      setSyncResult({
        success: true,
        message: `Successfully synced to Google Sheets`,
        details: data.details,
        timestamp: new Date()
      });
      
      console.log('Sync to sheets successful:', data);
    } catch (err) {
      console.error('Sync to sheets error:', err);
      setError(getErrorMessage(err));
      setSyncResult({
        success: false,
        message: 'Google Sheets sync failed',
        timestamp: new Date()
      });
    } finally {
      setSyncingToSheets(false);
      setSyncProgress(null);
    }
  };

  const getProgressColor = (phase: SyncProgress['phase']) => {
    switch (phase) {
      case 'fetching': return 'info';
      case 'processing': return 'warning';
      case 'updating': return 'primary';
      case 'cleaning': return 'secondary';
      case 'complete': return 'success';
      default: return 'primary';
    }
  };

  const getProgressIcon = (phase: SyncProgress['phase']) => {
    switch (phase) {
      case 'fetching': return <CloudDownload color="info" />;
      case 'processing': return <Refresh color="warning" />;
      case 'updating': return <Update color="primary" />;
      case 'cleaning': return <Refresh color="secondary" />;
      case 'complete': return <CheckCircle color="success" />;
      default: return <Refresh color="primary" />;
    }
  };

  const renderAccountInfo = (account: Account) => {
    const sourceFilterText = account.sourceFilter && account.sourceFilter.length > 0
      ? account.sourceFilter.join(', ')
      : 'All sources';
    
    const googleSheetsUrl = account.googleSheetsId 
      ? `https://docs.google.com/spreadsheets/d/${account.googleSheetsId}/edit`
      : null;
    
    return (
      <Box sx={{ mb: 2, p: 2, border: '1px solid #e0e0e0', borderRadius: 1 }}>
        <Typography variant="h6" gutterBottom>
          {account.name || 'Unnamed Account'}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Source Filter: {sourceFilterText}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Default Conversion Value: ${account.defaultConversionValue}
        </Typography>
        <Typography variant="body2" color="text.secondary">
          Auto Sync: {account.syncEnabled ? 'Enabled' : 'Disabled'}
          {account.syncEnabled && ` (${account.syncFrequency} at ${account.syncTime})`}
        </Typography>
        {account.lastSyncDate && (
          <Typography variant="body2" color="text.secondary">
            Last Sync: {new Date(account.lastSyncDate).toLocaleString()}
          </Typography>
        )}
        {googleSheetsUrl && (
          <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            📊 Google Sheet: {' '}
            <Link 
              href={googleSheetsUrl} 
              target="_blank" 
              rel="noopener noreferrer"
              sx={{ color: 'primary.main', textDecoration: 'none' }}
            >
              View Conversion Data
            </Link>
          </Typography>
        )}
      </Box>
    );
  };

  // Count jobs per account from allJobs using accountId
  const jobCounts: { [accountId: string]: number } = {};
  accounts.forEach(account => {
    if (account.id) {
      jobCounts[account.id] = allJobs.filter(job => String(job.accountId) === String(account.id)).length;
    }
  });

  if (!accounts || accounts.length === 0) {
    return (
      <Box sx={{ mt: 2 }}>
        <Alert severity="info">
          Please add an account to view jobs.
        </Alert>
      </Box>
    );
  }

  return (
    <Box sx={{ mt: 2 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6" component="div">
          Workiz Jobs
        </Typography>
        <FormControl sx={{ minWidth: 200 }}>
          <InputLabel>Select Account</InputLabel>
          <Select
            value={selectedAccount?.id || ''}
            label="Select Account"
            onChange={(e) => handleAccountChange(e.target.value)}
          >
            <MenuItem value="">Select an account</MenuItem>
            {accounts.map((account) => (
              <MenuItem key={account.id} value={account.id}>
                {account.name}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </Box>

      {selectedAccount && (
        <>
          {renderAccountInfo(selectedAccount)}
          
          {/* Sync Progress */}
          {syncProgress && (
            <Card sx={{ mb: 2, border: '1px solid', borderColor: `${getProgressColor(syncProgress.phase)}.main` }}>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                  {getProgressIcon(syncProgress.phase)}
                  <Typography variant="h6" sx={{ ml: 1, flexGrow: 1 }}>
                    {syncProgress.message}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    {syncProgress.percentage}%
                  </Typography>
                </Box>
                <LinearProgress 
                  variant="determinate" 
                  value={syncProgress.percentage} 
                  color={getProgressColor(syncProgress.phase)}
                  sx={{ height: 8, borderRadius: 4 }}
                />
                {syncProgress.details && (
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    {syncProgress.details}
                  </Typography>
                )}
              </CardContent>
            </Card>
          )}

          {/* Real-time Operation Progress */}
          {operationProgress && (
            <Card sx={{ mb: 2, border: '1px solid', borderColor: 'primary.main' }}>
              <CardContent>
                <Typography variant="h6" gutterBottom>
                  Parallel Operation Progress
                </Typography>
                <Box sx={{ mb: 2 }}>
                  <Typography variant="body2" color="text.secondary">
                    Status: {operationProgress.status}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    Accounts: {operationProgress.completedAccounts}/{operationProgress.totalAccounts} completed
                  </Typography>
                  {operationProgress.duration && (
                    <Typography variant="body2" color="text.secondary">
                      Duration: {Math.round(operationProgress.duration / 1000)}s
                    </Typography>
                  )}
                </Box>
                
                {/* Account-specific progress */}
                {operationProgress.accounts && (
                  <Box sx={{ mt: 2 }}>
                    <Typography variant="subtitle2" gutterBottom>
                      Account Progress:
                    </Typography>
                    {operationProgress.accounts.map((account: any, index: number) => (
                      <Box key={index} sx={{ mb: 1, p: 1, border: '1px solid #e0e0e0', borderRadius: 1 }}>
                        <Typography variant="body2" sx={{ fontWeight: 'bold' }}>
                          {account.accountName}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                          Status: {account.status} | 
                          Updated: {account.jobsUpdated} | 
                          Deleted: {account.jobsDeleted}
                        </Typography>
                        {account.errors && account.errors.length > 0 && (
                          <Typography variant="body2" color="error" sx={{ fontSize: '0.75rem' }}>
                            Errors: {account.errors.length}
                          </Typography>
                        )}
                      </Box>
                    ))}
                  </Box>
                )}
              </CardContent>
            </Card>
          )}

          {/* Sync Result */}
          {syncResult && (
            <Card sx={{ 
              mb: 2, 
              border: '1px solid', 
              borderColor: syncResult.success ? 'success.main' : 'error.main',
              backgroundColor: syncResult.success ? 'success.50' : 'error.50'
            }}>
              <CardContent>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                  {syncResult.success ? <CheckCircle color="success" /> : <ErrorIcon color="error" />}
                  <Typography variant="h6" sx={{ ml: 1, flexGrow: 1 }}>
                    {syncResult.message}
                  </Typography>
                  <IconButton 
                    size="small" 
                    onClick={() => setShowSyncDetails(!showSyncDetails)}
                  >
                    {showSyncDetails ? <ExpandLess /> : <ExpandMore />}
                  </IconButton>
                </Box>
                <Typography variant="body2" color="text.secondary">
                  {syncResult.timestamp.toLocaleString()}
                </Typography>
                
                <Collapse in={showSyncDetails}>
                  {syncResult.details && (
                    <Box sx={{ mt: 2, display: 'flex', flexWrap: 'wrap', gap: 1 }}>
                      {syncResult.details.jobsFromWorkiz !== undefined && (
                        <Chip 
                          label={`${syncResult.details.jobsFromWorkiz} from Workiz`} 
                          color="primary" 
                          size="small" 
                        />
                      )}
                      {syncResult.details.existingJobsFound !== undefined && (
                        <Chip 
                          label={`${syncResult.details.existingJobsFound} existing`} 
                          color="info" 
                          size="small" 
                        />
                      )}
                      {syncResult.details.finalJobCount !== undefined && (
                        <Chip 
                          label={`${syncResult.details.finalJobCount} total`} 
                          color="success" 
                          size="small" 
                        />
                      )}
                      {syncResult.details.jobsUpdated !== undefined && (
                        <Chip 
                          label={`${syncResult.details.jobsUpdated} updated`} 
                          color="warning" 
                          size="small" 
                        />
                      )}
                      {syncResult.details.jobsDeleted !== undefined && (
                        <Chip 
                          label={`${syncResult.details.jobsDeleted} deleted`} 
                          color="error" 
                          size="small" 
                        />
                      )}
                      {syncResult.details.failedUpdates !== undefined && (
                        <Chip 
                          label={`${syncResult.details.failedUpdates} failed`} 
                          color="error" 
                          size="small" 
                        />
                      )}
                    </Box>
                  )}
                </Collapse>
              </CardContent>
            </Card>
          )}

          {/* Resume Options */}
          {resumeInfo && resumeInfo.length > 0 && (
            <Card sx={{ mb: 2, border: '1px solid', borderColor: 'warning.main', backgroundColor: 'warning.50' }}>
              <CardContent>
                <Typography variant="h6" gutterBottom color="warning.main">
                  Resume Available
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  Some accounts have remaining jobs that can be processed. Click "Resume" to continue from where they left off.
                </Typography>
                <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                  {resumeInfo.map((account: any, index: number) => (
                    <Box key={index} sx={{ p: 2, border: '1px solid #e0e0e0', borderRadius: 1, backgroundColor: 'white' }}>
                      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <Box>
                          <Typography variant="subtitle1" sx={{ fontWeight: 'bold' }}>
                            {account.account}
                          </Typography>
                          <Typography variant="body2" color="text.secondary">
                            {account.resumeInfo.remainingJobs} jobs remaining | 
                            Last processed: {account.resumeInfo.lastProcessedJob}
                          </Typography>
                        </Box>
                        <Button
                          variant="outlined"
                          color="warning"
                          onClick={() => handleResumeUpdate(account.accountId || account.account, account.resumeInfo.lastProcessedJob)}
                          disabled={resuming}
                          size="small"
                        >
                          {resuming ? 'Resuming...' : 'Resume'}
                        </Button>
                      </Box>
                    </Box>
                  ))}
                </Box>
              </CardContent>
            </Card>
          )}

          {/* Three Main Action Buttons */}
          <Box sx={{ display: 'flex', gap: 2, mb: 2, flexWrap: 'wrap' }}>
            <Button
              variant="contained"
              color="primary"
              onClick={handleSyncLatestJobs}
              disabled={syncing || updatingJobs || syncingToSheets || resuming}
              startIcon={syncing ? <Refresh /> : <CloudDownload />}
              sx={{ minWidth: '150px' }}
            >
              {syncing ? 'Syncing...' : 'Sync Latest Jobs'}
            </Button>
            <Button
              variant="contained"
              color="secondary"
              onClick={handleUpdateAllJobs}
              disabled={syncing || updatingJobs || syncingToSheets || resuming}
              startIcon={updatingJobs ? <Refresh /> : <Update />}
              sx={{ minWidth: '150px' }}
            >
              {updatingJobs ? 'Updating...' : 'Update All Jobs (Parallel)'}
            </Button>
            <Button
              variant="contained"
              color="success"
              onClick={handleSyncToSheets}
              disabled={syncing || updatingJobs || syncingToSheets || resuming || !selectedAccount.googleSheetsId}
              startIcon={syncingToSheets ? <Refresh /> : <CloudUpload />}
              sx={{ minWidth: '150px' }}
            >
              {syncingToSheets ? 'Syncing...' : 'Sync to Google Sheets'}
            </Button>
          </Box>

          {/* Warnings and Info */}
          {!selectedAccount.workizApiToken && (
            <Alert severity="warning">
              Please add a Workiz API token to the selected account to sync jobs.
            </Alert>
          )}
          {!selectedAccount.googleSheetsId && (
            <Alert severity="warning">
              Please add a Google Sheet ID to sync jobs to Google Sheets.
            </Alert>
          )}
          <Alert severity="info" sx={{ mb: 2 }}>
            <AlertTitle>Automated Sync</AlertTitle>
            Jobs are automatically synced daily at 9:00 AM UTC via Vercel Cron Jobs. 
            Use the manual sync buttons above for immediate updates. The "Update All Jobs (Parallel)" 
            button processes all accounts simultaneously for faster completion.
          </Alert>
          {error && <Alert severity="error">{error}</Alert>}
        </>
      )}
    </Box>
  );
};

export default JobList;