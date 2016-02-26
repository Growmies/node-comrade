module.exports = {
  lockJob:                'UPDATE jobs SET (status, "workerMeta", "processingStartTime") = (\'processing\', $2, NOW()) WHERE id = $1 AND status = \'pending\' RETURNING id, payload',
  getPendingJobs:         'SELECT id as "jobId", payload FROM jobs WHERE (queue, status) = ($1, \'pending\')',
  getResults:             'SELECT result FROM jobs WHERE id = $1',
  createJob:              'INSERT INTO jobs (queue, payload, status, "insertionTime", result) VALUES ($1, $2, \'pending\', NOW(), \'{}\') RETURNING id',
  markJobAsDone:          'UPDATE jobs SET (status, result, "resultsMeta", "processingEndTime") = (\'done\', $2, $3, NOW()) WHERE id = $1',
  markJobAsDoneWithError: 'UPDATE jobs SET (status, result, "processingEndTime") = (\'error\', $2, NOW()) WHERE id = $1',
  deleteAllJobs:          'DELETE FROM "jobs"'
};

