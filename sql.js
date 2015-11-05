exports.findNextJob = (
  'SELECT * FROM jobs WHERE status = \'pending\' LIMIT 1 FOR UPDATE'
);

exports.commit = (
  'COMMIT'
);

exports.beginTransaction = (
  'BEGIN'
);

exports.endTransaction = (
  'ROLLBACK'
);

exports.lockJob = (
  'UPDATE jobs SET (status, "processingStartTime") = (\'processing\', NOW()) WHERE id = $1 AND status = \'pending\''
);

exports.createJob = (
  'INSERT INTO jobs (payload, status, "insertionTime", result) VALUES ($1, \'pending\', NOW(), \'{}\') RETURNING id'
);

exports.markJobAsDone = (
  'UPDATE jobs SET (status, result, "processingEndTime") = (\'done\', $2, NOW()) WHERE id = $1'
);

exports.markJobAsDoneWithError = (
  'UPDATE jobs SET (status, result, "processingEndTime") = (\'error\', $2, NOW()) WHERE id = $1'
);

exports.deleteAllJobs = (
  'DELETE FROM "jobs"'
);

