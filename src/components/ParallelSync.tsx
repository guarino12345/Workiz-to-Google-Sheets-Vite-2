import React, { useState, useEffect } from 'react';
import {
  Box,
  Button,
  Typography,
  Card,
  CardContent,
  LinearProgress,
  Chip,
  Alert,
  Accordion,
  AccordionSummary,
  AccordionDetails,
  CircularProgress
} from '@mui/material';
import {
  PlayArrow,
  Stop,
  Refresh,
  CheckCircle,
  Error,
  Schedule,
  Info,
  ExpandMore
} from '@mui/icons-material';
import { api } from '../utils/api';

interface SyncSession {
  sessionId: string;
  startTime: string;
  overallStatus: string;
  overallProgress: number;
  totalAccounts: number;
  completedAccounts: number;
  failedAccounts: number;
  processingAccounts: number;
  pendingAccounts: number;
  accounts: AccountSyncStatus[];
}

interface AccountSyncStatus {
  accountId: string;
  accountName: string;
  status: 'pending' | 'processing' | 'completed' | 'failed';
  progress: number;
  totalJobs: number;
  processedJobs: number;
  updatedJobs: number;
  failedJobs: number;
  startTime?: string;
  endTime?: string;
  duration?: number;
  error?: string;
}

interface SyncSessionSummary {
  sessionId: string;
  startTime: string;
  createdAt: string;
  totalAccounts: number;
  overallStatus: string;
  accounts: {
    accountName: string;
    status: string;
    progress: number;
  }[];
}

const ParallelSync: React.FC = () => {
  const [sessions, setSessions] = useState<SyncSessionSummary[]>([]);
  const [currentSession, setCurrentSession] = useState<SyncSession | null>(null);
  const [isInitializing, setIsInitializing] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [statusInterval, setStatusInterval] = useState<NodeJS.Timeout | null>(null);

  useEffect(() => {
    loadSessions();
    return () => {
      if (statusInterval) {
        clearInterval(statusInterval);
      }
    };
  }, []);

  const loadSessions = async () => {
    try {
      const response = await api.get('/api/sync/parallel/sessions');
      setSessions((response as any).data || []);
    } catch (error) {
      console.error('Failed to load sessions:', error);
    }
  };

  const initializeSync = async () => {
    setIsInitializing(true);
    setError(null);

    try {
      const response = await api.post('/api/sync/parallel/init', {});
      const { sessionId, accounts } = (response as any).data;

      console.log('Sync initialized:', (response as any).data);

      // Start processing all accounts in parallel
      await processAccounts(sessionId, accounts);

    } catch (error: any) {
      console.error('Failed to initialize sync:', error);
      setError(error.response?.data?.message || error.message);
    } finally {
      setIsInitializing(false);
    }
  };

  const processAccounts = async (sessionId: string, accounts: any[]) => {
    setIsProcessing(true);

    try {
      // Process all accounts in parallel
      const promises = accounts.map(account => 
        processAccount(sessionId, account.accountId)
      );

      await Promise.all(promises);

      // Start monitoring progress
      startStatusMonitoring(sessionId);

    } catch (error) {
      console.error('Failed to process accounts:', error);
      setError('Failed to process accounts');
    }
  };

  const processAccount = async (sessionId: string, accountId: string) => {
    try {
      const response = await api.post(`/api/sync/parallel/account/${accountId}`, {
        sessionId,
        batchSize: 29,
        delayMs: 2000 // 2-second delay between API calls (30 calls per minute)
      });

      console.log(`Account ${accountId} processed:`, (response as any).data);
      return (response as any).data;

    } catch (error: any) {
      console.error(`Failed to process account ${accountId}:`, error);
      throw error;
    }
  };

  const startStatusMonitoring = (sessionId: string) => {
    // Clear any existing interval
    if (statusInterval) {
      clearInterval(statusInterval);
    }

    // Start polling for status updates
    const interval = setInterval(async () => {
      try {
        const response = await api.get(`/api/sync/parallel/status/${sessionId}`);
        const session = (response as any).data;

        setCurrentSession(session);

        // Stop monitoring if all accounts are completed or failed
        if (session.overallStatus === 'completed' || 
            session.overallStatus === 'completed_with_errors' || 
            session.overallStatus === 'failed') {
          clearInterval(interval);
          setIsProcessing(false);
          loadSessions(); // Refresh sessions list
        }

      } catch (error) {
        console.error('Failed to get sync status:', error);
      }
    }, 5000); // Poll every 5 seconds

    setStatusInterval(interval);
  };

  const stopSync = () => {
    if (statusInterval) {
      clearInterval(statusInterval);
      setStatusInterval(null);
    }
    setIsProcessing(false);
    setCurrentSession(null);
  };

  const getStatusColor = (status: string): 'success' | 'primary' | 'error' | 'default' => {
    switch (status) {
      case 'completed': return 'success';
      case 'processing': return 'primary';
      case 'failed': return 'error';
      case 'pending': return 'default';
      default: return 'default';
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed': return <CheckCircle />;
      case 'processing': return <CircularProgress size={16} />;
      case 'failed': return <Error />;
      case 'pending': return <Schedule />;
      default: return <Info />;
    }
  };

  const formatDuration = (ms: number) => {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);

    if (hours > 0) {
      return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds % 60}s`;
    } else {
      return `${seconds}s`;
    }
  };

  return (
    <Box sx={{ p: 3 }}>
      <Typography variant="h4" gutterBottom>
        Parallel Account Sync
      </Typography>
      
      <Typography variant="body1" color="text.secondary" sx={{ mb: 3 }}>
        Process all accounts in parallel with batch UUID updates. Each account respects its own API rate limits.
      </Typography>

      {/* Control Buttons */}
      <Box sx={{ mb: 3 }}>
        <Button
          variant="contained"
          startIcon={<PlayArrow />}
          onClick={initializeSync}
          disabled={isInitializing || isProcessing}
          sx={{ mr: 2 }}
        >
          {isInitializing ? 'Initializing...' : 'Start Parallel Sync'}
        </Button>

        {isProcessing && (
          <Button
            variant="outlined"
            startIcon={<Stop />}
            onClick={stopSync}
            color="error"
          >
            Stop Sync
          </Button>
        )}

        <Button
          variant="outlined"
          startIcon={<Refresh />}
          onClick={loadSessions}
          sx={{ ml: 2 }}
        >
          Refresh Sessions
        </Button>
      </Box>

      {/* Error Display */}
      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      {/* Current Session Progress */}
      {currentSession && (
        <Card sx={{ mb: 3 }}>
          <CardContent>
            <Box sx={{ display: 'flex', alignItems: 'center', mb: 2 }}>
              <Typography variant="h6" sx={{ flexGrow: 1 }}>
                Current Sync Session: {currentSession.sessionId}
              </Typography>
              <Chip
                label={currentSession.overallStatus}
                color={getStatusColor(currentSession.overallStatus)}
                icon={getStatusIcon(currentSession.overallStatus)}
              />
            </Box>

            <Box sx={{ mb: 2 }}>
              <Typography variant="body2" color="text.secondary">
                Overall Progress: {currentSession.overallProgress}%
              </Typography>
              <LinearProgress 
                variant="determinate" 
                value={currentSession.overallProgress} 
                sx={{ mt: 1 }}
              />
            </Box>

            <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: 2, mb: 2 }}>
              <Box>
                <Typography variant="body2" color="text.secondary">
                  Total Accounts
                </Typography>
                <Typography variant="h6">
                  {currentSession.totalAccounts}
                </Typography>
              </Box>
              <Box>
                <Typography variant="body2" color="text.secondary">
                  Completed
                </Typography>
                <Typography variant="h6" color="success.main">
                  {currentSession.completedAccounts}
                </Typography>
              </Box>
              <Box>
                <Typography variant="body2" color="text.secondary">
                  Processing
                </Typography>
                <Typography variant="h6" color="primary.main">
                  {currentSession.processingAccounts}
                </Typography>
              </Box>
              <Box>
                <Typography variant="body2" color="text.secondary">
                  Failed
                </Typography>
                <Typography variant="h6" color="error.main">
                  {currentSession.failedAccounts}
                </Typography>
              </Box>
            </Box>

            {/* Account Details */}
            <Accordion>
              <AccordionSummary expandIcon={<ExpandMore />}>
                <Typography variant="subtitle1">
                  Account Details ({currentSession.accounts.length} accounts)
                </Typography>
              </AccordionSummary>
              <AccordionDetails>
                <Box sx={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', gap: 2 }}>
                  {currentSession.accounts.map((account) => (
                    <Box key={account.accountId}>
                      <Card variant="outlined">
                        <CardContent>
                          <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                            <Typography variant="subtitle2" sx={{ flexGrow: 1 }}>
                              {account.accountName}
                            </Typography>
                            <Chip
                              label={account.status}
                              color={getStatusColor(account.status)}
                              size="small"
                              icon={getStatusIcon(account.status)}
                            />
                          </Box>

                          {account.totalJobs > 0 && (
                            <Box sx={{ mb: 1 }}>
                              <Typography variant="body2" color="text.secondary">
                                Jobs: {account.processedJobs}/{account.totalJobs} ({account.progress}%)
                              </Typography>
                              <LinearProgress 
                                variant="determinate" 
                                value={account.progress} 
                                sx={{ mt: 1 }}
                              />
                            </Box>
                          )}

                          {account.updatedJobs > 0 && (
                            <Typography variant="body2" color="success.main">
                              Updated: {account.updatedJobs} jobs
                            </Typography>
                          )}

                          {account.failedJobs > 0 && (
                            <Typography variant="body2" color="error.main">
                              Failed: {account.failedJobs} jobs
                            </Typography>
                          )}

                          {account.duration && (
                            <Typography variant="body2" color="text.secondary">
                              Duration: {formatDuration(account.duration)}
                            </Typography>
                          )}

                          {account.error && (
                            <Typography variant="body2" color="error.main">
                              Error: {account.error}
                            </Typography>
                          )}
                        </CardContent>
                      </Card>
                    </Box>
                  ))}
                </Box>
              </AccordionDetails>
            </Accordion>
          </CardContent>
        </Card>
      )}

      {/* Previous Sessions */}
      {sessions.length > 0 && (
        <Card>
          <CardContent>
            <Typography variant="h6" gutterBottom>
              Previous Sync Sessions
            </Typography>
            
            {sessions.map((session) => (
              <Box key={session.sessionId} sx={{ mb: 2, p: 2, border: '1px solid #e0e0e0', borderRadius: 1 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                  <Typography variant="subtitle2" sx={{ flexGrow: 1 }}>
                    {session.sessionId}
                  </Typography>
                  <Chip
                    label={session.overallStatus}
                    color={getStatusColor(session.overallStatus)}
                    size="small"
                  />
                </Box>
                
                <Typography variant="body2" color="text.secondary">
                  Started: {new Date(session.startTime).toLocaleString()}
                </Typography>
                
                <Typography variant="body2" color="text.secondary">
                  Accounts: {session.totalAccounts}
                </Typography>

                <Box sx={{ mt: 1 }}>
                  {session.accounts.map((account, index) => (
                    <Chip
                      key={index}
                      label={`${account.accountName}: ${account.progress}%`}
                      color={getStatusColor(account.status)}
                      size="small"
                      sx={{ mr: 1, mb: 1 }}
                    />
                  ))}
                </Box>
              </Box>
            ))}
          </CardContent>
        </Card>
      )}
    </Box>
  );
};

export default ParallelSync; 