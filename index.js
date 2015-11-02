var pg      = require('pg');
var _       = require('lodash');
var Promise = require('bluebird');
var sql     = require('./sql');


function Comrade(connectionOptions, cb, id) {
  var self          = this;
  self.id           = id;
  self.queue        = [];
  cb                = cb || _.noop;
  this.jobCallbacks = {};

  pg.connect(connectionOptions, function(err, client) {
    if (err) return cb(err);
    self.client = client;
    return cb(null, client);
  });

  this.destroy = function() {
    this.client.end();
  },

  this.watchForJobs = function(workerFunction) {
    var self = this;
    self.workerFunction = workerFunction;

    this.client.query('LISTEN "jobChange"');

    this.client.on('notification', function(notification) {
      var parts   = notification.payload.split('::');
      var jobId   = parseInt(parts[0]);
      var payload = JSON.parse(parts[1]);
      var status  = parts[2];
      var result  = JSON.parse(parts[3]);

      switch(status) {
        case 'pending':
          self.attemptToProcess({
            jobId: jobId,
            payload: payload,
            status: status,
            result: result
          });
          break;
        case 'done':
          if (self.jobCallbacks[jobId]) {
           self.jobCallbacks[jobId](null, result);
          }
          delete self.jobCallbacks[jobId];
          self.checkQueueForJobs();
          break;
        case 'error':
          if (self.jobCallbacks[jobId]) {
           self.jobCallbacks[jobId](result);
          }
          delete self.jobCallbacks[jobId];
          self.checkQueueForJobs();
          break;
      }
    });
  },

  this.attemptToProcess = function(job) {
    if (self.locked) {
      self.queue.push(job);
      return;
    }

    self.locked = true;
    self.lockJob(job.jobId, function(err, gotLock) {
      if (err) return console.log(self.id, 'There was a big error', err);
      if (gotLock) {
        self.locked = false;
        self.workerFunction(job.payload, function(err, result) {
          if (err) return self.markJobAsDoneWithError(job.jobId, err);
          self.markJobAsDone(job.jobId, result);
        });
      } else {
        self.checkQueueForJobs();
      }
    });
  },

  this.checkQueueForJobs = function() {
    self.locked = false;
    var job = self.queue.pop();
    if (job) {
      self.attemptToProcess(job) ;
    }
  },

  this.markJobAsDone = function(jobId, result) {
    this.client.query(sql.markJobAsDone, [jobId, result], function(err) {
      if (err) {
        console.log('This should definitly do something...') ;
      }
    });
  };

  this.markJobAsDoneWithError = function(jobId, err) {
    this.client.query(sql.markJobAsDoneWithError, [jobId, { error: err.message || err }], function(err) {
      if (err) {
        console.log('This should definitly do something...') ;
      }
    });
  };

  this.createJob = function(job, cb) {
    this.client.query(sql.createJob, [job], function(err, results) {
      if (err) return cb(err);
      self.jobCallbacks[results.rows[0].id] = cb;
    });
  }

  this.lockJob = function(jobId, cb) {
    this.client.query(sql.beginTransaction, function(err) {
      if (err) return cb(err);
      self.client.query(sql.lockJob, [jobId], function(err, results) {
        if (err) return self.rollback(cb.bind(err));
        self.client.query(sql.commit, function() {
          cb(null, results.rowCount);
        });
      });
    });
  };

  this.rollback = function(cb) {
    this.client.query(sql.rollback, function() {
      cb();
    });
  }

  this.deleteAllJobs = function(cb) {
    this.client.query(sql.deleteAllJobs, cb)
  }
}

module.exports = {
  Comrade: Comrade
};
