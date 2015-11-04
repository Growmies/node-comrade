var pg      = require('pg');
var _       = require('lodash');
var Promise = require('bluebird');
var sql     = require('./sql');


function Consumer(connectionOptions, cb, id) {
  var self          = this;
  self.id           = id;
  cb                = cb || _.noop;
  this.jobPromises  = {};
  this.listening    = false;

  pg.connect(connectionOptions, function(err, client) {
    if (err) return cb(err);
    self.client = client;
    return cb(null, client);
  });

  this.destroy = function() {
    this.client.end();
  };

  this.createJob = function(job, cb) {
    return new Promise(function(resolve, reject) {
      self.client.query(sql.createJob, [job], function(err, results) {
        if (err) return reject(err);
        self.jobPromises[results.rows[0].id] = { resolve: resolve, reject: reject };
        if (!self.listening) {
          self.listen();
        }
      });
    })
  };

  this.listen = function() {
    this.client.query('LISTEN "jobChange"');
    this.client.on('notification', function(notification) {
      var parts   = notification.payload.split('::');
      var jobId   = parseInt(parts[0]);
      var payload = JSON.parse(parts[1]);
      var status  = parts[2];
      var result  = JSON.parse(parts[3]);


      if (!_.contains(['done', 'error'], status)) return;
      if (!self.jobPromises[jobId]) return;

      var method = status === 'done' ? self.jobPromises[jobId].resolve : self.jobPromises[jobId].reject;
      method(result);
      delete self.jobPromises[jobId];
    });
  };
}

function Worker(connectionOptions, cb, id) {
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

      if (status === 'pending') {
        self.attemptToProcess({
          jobId: jobId,
          payload: payload,
          status: status,
          result: result
        });
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
        self.checkQueueForJobs();
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
  Worker: Worker,
  Consumer: Consumer
};
