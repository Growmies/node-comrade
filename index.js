var pg      = require('pg');
var _       = require('lodash');
var Promise = require('bluebird');
var sql     = require('./sql');

function Producer(connectionOptions, cb, id) {
  var self          = this;
  self.id           = id;
  cb                = cb || _.noop;
  this.jobPromises  = {};

  pg.connect(connectionOptions, function(err, client) {
    if (err) return cb(err);
    self.client = client;
    self.listen();
    return cb(null, client);
  });

  this.destroy = function() {
    this.client.end();
  };

  this.createJob = function(queueName, job) {
    return new Promise(function(resolve, reject) {
      self.client.query(sql.createJob, [queueName, job], function(err, results) {
        if (err) {
          return reject(err);
        }
        var jobId = results.rows[0].id;
        self.jobPromises[jobId] = {};
        self.jobPromises[jobId].promise = new Promise(function(jobPromiseResolve, jobPromiseReject) {
                                                          self.jobPromises[jobId].resolve = jobPromiseResolve;
                                                          self.jobPromises[jobId].reject  = jobPromiseReject;
                                                       });
        return resolve(self.jobPromises[jobId].promise);
      });
    });
  };

  this.getJobPromiseById = function(jobId) {
    return _.get(self.jobPromises[jobId], 'promise');
  }

  this.listen = function() {
    var self = this;
    this.client.query('LISTEN "jobComplete"');
    this.client.on('notification', function(notification) {
      var parts   = notification.payload.split('::');
      var jobId   = parseInt(parts[0]);
      var status  = parts[1];

      // Normally, we would return the result in the notification, but sometimes it is just too big and postgres fails.
      if (!self.jobPromises[jobId]) return;
      self.client.query(sql.getResults, [jobId], function(err, results) {
        if (err) return self.jobPromises[jobId].reject({ error: err.message });
        var result = results.rows[0].result;
        if (status === 'done') {
          self.jobPromises[jobId].resolve(result);
        } else {
          self.jobPromises[jobId].reject(result);
        }
        delete self.jobPromises[jobId];
      });
    });
  };
}

function Consumer(connectionOptions, cb, options) {
  var self               = this;

  cb                     = cb                        || _.noop;
  self.id                = options.id                || 0;
  self.maxConcurrentJobs = options.maxConcurrentJobs || 10;

  self.currentJobs       = 0;
  self.backLog           = [];

  pg.connect(connectionOptions, function(err, client) {
    if (err) return cb(err);
    self.client = client;
    return cb(null, client);
  });

  this.destroy = function() {
    this.client.end();
  };

  this.watchForJobs = function(queueName, workerFunction, workerMeta) {
    var self            = this;
    this.queueName      = queueName;
    this.workerFunction = workerFunction;
    this.workerMeta     = workerMeta || {};

    this.client.query('LISTEN "newJob"');

    this.client.on('notification', function(notification) {
      var parts     = notification.payload.split('::');
      var jobId     = parseInt(parts[0]);
      var queueName = parts[1];

      if (queueName !== self.queueName) {
        return;
      }

      self.client.query(sql.getPayload, [jobId], function(err, results) {
        if (err) return;
        var payload = results.rows[0].payload;

        self.attemptToProcess({
          jobId: jobId,
          payload: payload
        });
      });
    });

    this.checkDbForPendingJobs();
  };

  this.checkDbForPendingJobs = function() {
    var self = this;
    this.client.query(sql.getPendingJobs, [this.queueName], function(err, results) {
      _.each(results.rows, function(pendingJob) {
        self.attemptToProcess(pendingJob);
      });
    });
  };

  this.attemptToProcess = function(job) {
    if (self.locked || self.currentJobs >= self.maxConcurrentJobs) {
      self.backLog.push(job);
      return;
    }

    self.locked = true;
    self.lockJob(job.jobId, self.workerMeta, function(err, gotLock) {
      if (err) return self.markJobAsDoneWithError(job.jobId, err);
      if (gotLock) {
        self.currentJobs++;
        self.workerFunction(job.payload, function(err, result, resultsMeta) {
          self.currentJobs--;
          if (err) {
            self.markJobAsDoneWithError(job.jobId, err);
          } else {
            self.markJobAsDone(job.jobId, result, resultsMeta);
          }
          self.checkBacklogForJobs();
        });
      }
      self.checkBacklogForJobs();
    });
  };

  this.checkBacklogForJobs = function() {
    self.locked = false;
    var job = self.backLog.pop();
    if (job) {
      _.defer(function() {
        self.attemptToProcess(job);
      });
    }
  };

  this.lockJob = function(jobId, workerMeta, cb) {
    self.client.query(sql.lockJob, [jobId, workerMeta], function(err, results) {
      if (err) return cb(err);
      cb(err, results.rowCount);
    });
  };

  this.markJobAsDone = function(jobId, result, resultsMeta) {
    var self = this;
    self.client.query(sql.markJobAsDone, [jobId, result, resultsMeta], function(err) {
      if (err) {
        console.error('Could not mark done', err.stack);
      }
    });
  };

  this.markJobAsDoneWithError = function(jobId, err) {
    this.client.query(sql.markJobAsDoneWithError, [jobId, { error: err.message || err }], function(err) {
      if (err) {
        console.error('Could not mark done with an error');
      }
    });
  };

  this.deleteAllJobs = function(cb) {
    this.client.query(sql.deleteAllJobs, cb);
  };
}

module.exports = {
  Consumer: Consumer,
  Producer: Producer
};
