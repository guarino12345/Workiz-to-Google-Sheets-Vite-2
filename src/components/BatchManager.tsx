import React, { useState } from 'react';
import { api } from '../utils/api';

interface BatchManagerProps {
  operationId?: string;
}

interface BatchStats {
  totalBatches: number;
  completedBatches: number;
  failedBatches: number;
  pendingBatches: number;
  processingBatches: number;
}

export const BatchManager: React.FC<BatchManagerProps> = ({ operationId }) => {
  const [isLoading, setIsLoading] = useState(false);
  const [message, setMessage] = useState('');
  const [error, setError] = useState('');
  const [stats, setStats] = useState<BatchStats | null>(null);

  const setupIndexes = async () => {
    setIsLoading(true);
    setError('');
    setMessage('');
    
    try {
      const response = await api.post('/setup-indexes', {});
      setMessage(`✅ ${(response as any).data.message}`);
    } catch (err: any) {
      setError(`❌ Failed to setup indexes: ${err.response?.data?.error || err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const cleanupStaleBatches = async () => {
    setIsLoading(true);
    setError('');
    setMessage('');
    
    try {
      const response = await api.post('/cleanup-stale-batches', {});
      setMessage(`✅ ${(response as any).data.message}`);
    } catch (err: any) {
      setError(`❌ Failed to cleanup stale batches: ${err.response?.data?.error || err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  const getBatchStats = async () => {
    if (!operationId) {
      setError('No operation ID provided');
      return;
    }
    
    setIsLoading(true);
    setError('');
    
    try {
      const response = await api.get(`/batch-progress/${operationId}`);
      const { batches } = (response as any).data;
      
      const stats: BatchStats = {
        totalBatches: batches.length,
        completedBatches: batches.filter((b: any) => b.status === 'completed').length,
        failedBatches: batches.filter((b: any) => b.status === 'failed').length,
        pendingBatches: batches.filter((b: any) => b.status === 'pending').length,
        processingBatches: batches.filter((b: any) => b.status === 'processing').length,
      };
      
      setStats(stats);
    } catch (err: any) {
      setError(`❌ Failed to get batch stats: ${err.response?.data?.error || err.message}`);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <h2 className="text-2xl font-bold mb-6 text-gray-800">Batch Processing Manager</h2>
      
      {/* Setup Section */}
      <div className="mb-8">
        <h3 className="text-lg font-semibold mb-4 text-gray-700">Database Setup</h3>
        <div className="space-y-3">
          <button
            onClick={setupIndexes}
            disabled={isLoading}
            className="bg-blue-500 hover:bg-blue-600 disabled:bg-blue-300 text-white px-4 py-2 rounded-md transition-colors"
          >
            {isLoading ? 'Setting up...' : 'Setup Database Indexes'}
          </button>
          <p className="text-sm text-gray-600">
            Creates database indexes for optimal batch processing performance
          </p>
        </div>
      </div>

      {/* Cleanup Section */}
      <div className="mb-8">
        <h3 className="text-lg font-semibold mb-4 text-gray-700">Maintenance</h3>
        <div className="space-y-3">
          <button
            onClick={cleanupStaleBatches}
            disabled={isLoading}
            className="bg-orange-500 hover:bg-orange-600 disabled:bg-orange-300 text-white px-4 py-2 rounded-md transition-colors"
          >
            {isLoading ? 'Cleaning up...' : 'Cleanup Stale Batches'}
          </button>
          <p className="text-sm text-gray-600">
            Resets batches that have been processing for more than 1 hour
          </p>
        </div>
      </div>

      {/* Stats Section */}
      {operationId && (
        <div className="mb-8">
          <h3 className="text-lg font-semibold mb-4 text-gray-700">Batch Statistics</h3>
          <div className="space-y-3">
            <button
              onClick={getBatchStats}
              disabled={isLoading}
              className="bg-green-500 hover:bg-green-600 disabled:bg-green-300 text-white px-4 py-2 rounded-md transition-colors"
            >
              {isLoading ? 'Loading...' : 'Get Batch Stats'}
            </button>
            
            {stats && (
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mt-4">
                <div className="bg-gray-50 p-3 rounded-md text-center">
                  <div className="text-2xl font-bold text-gray-800">{stats.totalBatches}</div>
                  <div className="text-sm text-gray-600">Total</div>
                </div>
                <div className="bg-green-50 p-3 rounded-md text-center">
                  <div className="text-2xl font-bold text-green-600">{stats.completedBatches}</div>
                  <div className="text-sm text-gray-600">Completed</div>
                </div>
                <div className="bg-red-50 p-3 rounded-md text-center">
                  <div className="text-2xl font-bold text-red-600">{stats.failedBatches}</div>
                  <div className="text-sm text-gray-600">Failed</div>
                </div>
                <div className="bg-yellow-50 p-3 rounded-md text-center">
                  <div className="text-2xl font-bold text-yellow-600">{stats.pendingBatches}</div>
                  <div className="text-sm text-gray-600">Pending</div>
                </div>
                <div className="bg-blue-50 p-3 rounded-md text-center">
                  <div className="text-2xl font-bold text-blue-600">{stats.processingBatches}</div>
                  <div className="text-sm text-gray-600">Processing</div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Messages */}
      {message && (
        <div className="bg-green-50 border border-green-200 text-green-800 px-4 py-3 rounded-md mb-4">
          {message}
        </div>
      )}
      
      {error && (
        <div className="bg-red-50 border border-red-200 text-red-800 px-4 py-3 rounded-md mb-4">
          {error}
        </div>
      )}

      {/* Info Section */}
      <div className="bg-blue-50 border border-blue-200 rounded-md p-4">
        <h4 className="font-semibold text-blue-800 mb-2">How it works:</h4>
        <ul className="text-sm text-blue-700 space-y-1">
          <li>• <strong>Cron Job:</strong> Runs every 5 minutes to process pending batches</li>
          <li>• <strong>Batch Size:</strong> 10 jobs per batch to avoid timeouts</li>
          <li>• <strong>Parallel Processing:</strong> Multiple accounts processed simultaneously</li>
          <li>• <strong>Sequential Per Account:</strong> Batches for each account processed one at a time</li>
          <li>• <strong>Auto Recovery:</strong> Stale batches are automatically reset</li>
        </ul>
      </div>
    </div>
  );
}; 