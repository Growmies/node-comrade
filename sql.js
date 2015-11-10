module.exports = {
  findNextJob:            'SELECT * FROM jobs WHERE (queue, status) = ($1, \'pending\') LIMIT 1 FOR UPDATE',
  getPendingJobs:         'SELECT id as "jobId", payload FROM jobs WHERE (queue, status) = ($1, \'pending\')',
  getResults:             'SELECT result FROM jobs WHERE id = $1',
  lockJob:                'UPDATE jobs SET (status, "processingStartTime") = (\'processing\', NOW()) WHERE id = $1 AND status = \'pending\'',
  createJob:              'INSERT INTO jobs (queue, payload, status, "insertionTime", result) VALUES ($1, $2, \'pending\', NOW(), \'{}\') RETURNING id',
  markJobAsDone:          'UPDATE jobs SET (status, result, "processingEndTime") = (\'done\', $2, NOW()) WHERE id = $1',
  markJobAsDoneWithError: 'UPDATE jobs SET (status, result, "processingEndTime") = (\'error\', $2, NOW()) WHERE id = $1',
  deleteAllJobs:          'DELETE FROM "jobs"',
  commit:                 'COMMIT',
  beginTransaction:       'BEGIN',
  endTransaction:         'ROLLBACK',
}



