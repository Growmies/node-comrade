var expect  = require('chai').expect;
var pg      = require('pg');
var _       = require('lodash');
var comrade = require('../');
var Consumer;
var Producer;

beforeEach(function(done) {
  var iAmDone = _.after(2, done);

  Consumer = new comrade.Consumer('postgres://localhost/postgres', function(err, client) {
    Consumer.deleteAllJobs(function(err, results) {
      iAmDone();
    });
  }, { id: 0 });

  Producer = new comrade.Producer('postgres://localhost/postgres', function(err, client) {
    iAmDone();
  }, { id: 0 });
});

afterEach(function() {
  Consumer.destroy();
  Producer.destroy();
});

describe('Comrade job processor', function() {
  it('Should pick up a job', function(done) {
    Consumer.watchForJobs('default', function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, 10);
    });

    Producer.createJob('default', { input: 4})
    .then(function(results) {
      expect(results.result).to.equal(8);
      done();
    });
  });

  it('Should pick up multiple jobs', function(done) {
    Consumer.watchForJobs('default', function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, payload.timeout);
    });

    var iAmDone = _.after(3, done);

    Producer.createJob('default', { input: 4, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(8);
      iAmDone();
    });

    Producer.createJob('default', { input: 5, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(10);
      iAmDone();
    });

    Producer.createJob('default', { input: 6, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(12);
      iAmDone();
    });
  });

  it('Should properly handle errors', function(done) {
    Consumer.watchForJobs('default', function workerErrorFunc(payload, cb) {
      setTimeout(function() {
        cb(new Error('There was a big error'));
      }, payload.timeout);
    });

    Producer.createJob('default', { timeout: 0 })
    .then(function(results) {
      done('This should have thrown an error.');
    })
    .catch(function(err) {
      expect(err).to.not.be.a('null');
      done();
    });
  });

  it('Work should be somwhat distributed amongst various workers', function(done) {
    this.timeout(5000);
    var allConnected = _.after(4, function() {
      var representedServers = [];
      var numbersToProcess = _.range(1, 100);

      var iAmDone = _.after(numbersToProcess.length, function() {
        expect(_.uniq(representedServers)).to.have.length.above(1);
        Consumer1.destroy();
        Consumer2.destroy();
        Consumer3.destroy();
        Consumer4.destroy();
        done();
      });

      Consumer1.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        setTimeout(function() {
          cb(null, {result: payload.input * 2, workerId: 1 });
        }, payload.input);
      });
      Consumer2.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        setTimeout(function() {
          cb(null, {result: payload.input * 2, workerId: 2 });
        }, payload.input);
      });
      Consumer3.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        setTimeout(function() {
          cb(null, {result: payload.input * 2, workerId: 3 });
        }, payload.input);
      });
      Consumer4.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        setTimeout(function() {
          cb(null, {result: payload.input * 2, workerId: 4 });
        }, payload.input);
      });

      _.each(numbersToProcess, function(num) {
        (function(num) {
          setTimeout(function() {
            Producer.createJob('default', { input: num })
            .then(function(results) {
              representedServers.push(results.workerId);
              expect(results.result).to.equal(num * 2);
              iAmDone();
            });
          }, num * 10);
        })(num);
      });
    });

    var Consumer1 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 1, maxConcurrentJobs: 20 });
    var Consumer2 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 2, maxConcurrentJobs: 20 });
    var Consumer3 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 3, maxConcurrentJobs: 20 });
    var Consumer4 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 4, maxConcurrentJobs: 20 });
  });

  it('Should be able to attach to an existing job\'s promise', function(done) {
    pg.connect('postgres://localhost/postgres', function(err, client) {
      if (err) return cb(err);

      var run = function() {
        Consumer.watchForJobs('existingJobTest', function workerErrorFunc(payload, cb) {
          setTimeout(function() {
            cb(null, payload.input * 2);
          }, 500);
        });

        var allComplete = _.after(2, function() {
          Producer.destroy();
          Consumer.destroy();
          done();
        });

        Producer.createJob('existingJobTest', { input: 5 })
        .then(function(result) {
          expect(result).to.equal(10);
          allComplete();
        });

        setTimeout(function() {
          client.query('SELECT id FROM jobs ORDER BY id LIMIT 1', null, function(err, results) {
            Producer.getJobPromiseById(results.rows[0].id)
            .then(function(results) {
              expect(results).to.equal(10);
              allComplete();
            });
          });
        }, 50);
      };

      var Consumer = new comrade.Consumer('postgres://localhost/postgres', run, { id: 1, maxConcurrentJobs: 20 });
    });
  });

  it('Work done by different instances on different queues', function(done) {
    this.timeout(5000);
    var allConnected = _.after(4, function() {
      var iAmDone = _.after(4, function() {
        Consumer1.destroy();
        Consumer2.destroy();
        Consumer3.destroy();
        Consumer4.destroy();
        done();
      });

      Consumer1.watchForJobs('adder', function adder(payload, cb) {
        cb(null, { result: payload.val1 + payload.val2 }, { workerId: 'consumer1'});
      }, { id: 'consumer1' });
      Consumer2.watchForJobs('subtractor', function subtractor(payload, cb) {
        cb(null, { result: payload.val1 - payload.val2 }, { workerId: 'consumer2'});
      }, { id: 'consumer2' });
      Consumer3.watchForJobs('multiplier', function multiplier(payload, cb) {
        cb(null, { result: payload.val1 * payload.val2 }, { workerId: 'consumer3'});
      }, { id: 'consumer3' });
      Consumer4.watchForJobs('divider', function divider(payload, cb) {
        cb(null, { result: payload.val1 / payload.val2 }, { workerId: 'consumer4'});
      }, { id: 'consumer4' });

      Producer.createJob('adder', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(15);
        iAmDone();
      });
      Producer.createJob('subtractor', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(5);
        iAmDone();
      });
      Producer.createJob('multiplier', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(50);
        iAmDone();
      });
      Producer.createJob('divider', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(2);
        iAmDone();
      });

    });

    var Consumer1 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 1 });
    var Consumer2 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 2 });
    var Consumer3 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 3 });
    var Consumer4 = new comrade.Consumer('postgres://localhost/postgres', allConnected, { id: 4 });
  });

  // it('Should make sure that the max concurrent workers is respected', function(done) {
  //   this.timeout(5000);
  //   var run = function() {

  //     Consumer.watchForJobs('testMaxConcurrentWorkers', function workerErrorFunc(payload, cb) {
  //       setTimeout(function() {
  //         cb(null, payload.input);
  //       }, payload.input);
  //     });

  //     var allComplete = _.after(2, function() {
  //       Producer.destroy();
  //       Consumer.destroy();
  //       done();
  //     });

  //     Producer.createJob('testMaxConcurrentWorkers', { input: 5 })
  //     .then(function(result) {
  //       expect(result).to.equal(5);
  //       allComplete();
  //     });
  //   }

  //   var Consumer = new comrade.Consumer('postgres://localhost/postgres', run, { id: 1, maxConcurrentJobs: 20 });
  // });
});


