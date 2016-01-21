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
        if (err) return reject(err);
        self.jobPromises[results.rows[0].id] = { resolve: resolve, reject: reject };
      });
    });
  };

  this.listen = function() {
    var self = this;
    this.client.query('LISTEN "jobComplete"');
    this.client.on('notification', function(notification) {
      var parts   = notification.payload.split('::');
      var jobId   = parseInt(parts[0]);
      var status  = parts[1];

      // Normally, we would return the result in the notification, but sometimes it is just too big and postgres fails.
      // Not as awesome, but it is reliable
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

function Consumer(connectionOptions, cb, id, cyclicOffset, totalConsumers) {
  var self            = this;
  self.id             = id || 0;
  self.cyclicOffset   = cyclicOffset || 0;
  self.totalConsumers = totalConsumers || 0;
  self.backLog        = [];
  cb                  = cb || _.noop;

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
      setTimeout(function() {
        console.log('++self.cyclicOffset % self.totalConsumers', ++self.cyclicOffset % self.totalConsumers);
        var parts   = notification.payload.split('::');
        var jobId   = parseInt(parts[0]);
        var queueName   = parts[1];

        if (queueName !== self.queueName) { // This is not the job we are looking for
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
      }, ++self.cyclicOffset % self.totalConsumers); // This makes each worker delay trying to get the job in a cyclic amount. 0, 1, 2, 3, 0, 1, 2, 3 - in an attempt at making the load more even.
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
    if (self.locked) {
      self.backLog.push(job);
      return;
    }

    self.locked = true;
    self.lockJob(job.jobId, self.workerMeta, function(err, gotLock) {
      if (err) return self.markJobAsDoneWithError(job.jobId, err);
      if (gotLock) {
        self.workerFunction(job.payload, function(err, result, resultsMeta) {
          if (err) return self.markJobAsDoneWithError(job.jobId, err);
          self.markJobAsDone(job.jobId, result, resultsMeta);
        });
      }
      self.checkBacklogForJobs();
    });
  };

  this.checkBacklogForJobs = function() {
    self.locked = false;
    var job = self.backLog.pop();
    if (job) {
      self.attemptToProcess(job);
    }
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

  this.lockJob = function(jobId, workerMeta, cb) {
    self.client.query(sql.lockJob, [jobId, workerMeta], function(err, results) {
      if (err) return cb(err);
      cb(err, results.rowCount);
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
