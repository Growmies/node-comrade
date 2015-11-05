var expect  = require('chai').expect;
var comrade = require('../');
var _       = require('lodash');
var Worker;
var Consumer;

beforeEach(function(done) {
  var iAmDone = _.after(2, done);

  Worker = new comrade.Worker('postgres://localhost/postgres', function(err, client) {
    Worker.deleteAllJobs(function(err, results) {
      iAmDone()
    });
  }, 0);

  Consumer = new comrade.Consumer('postgres://localhost/postgres', function(err, client) {
    iAmDone();
  }, 0);
});

afterEach(function() {
  Worker.destroy();
  Consumer.destroy();
});

describe('Comrade job processor', function() {
  it('Should pick up a job', function(done) {
    Worker.watchForJobs('default', function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, 10);
    });

    Consumer.createJob('default', { input: 4})
    .then(function(results) {
      expect(results.result).to.equal(8);
      done();
    });

  });

  it('Should pick up multiple jobs', function(done) {
    Worker.watchForJobs('default', function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, payload.timeout);
    });

    var iAmDone = _.after(3, done);

    Consumer.createJob('default', { input: 4, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(8);
      iAmDone();
    });

    Consumer.createJob('default', { input: 5, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(10);
      iAmDone();
    });

    Consumer.createJob('default', { input: 6, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(12);
      iAmDone();
    });
  });

  it('Should properly handle errors', function(done) {
    Worker.watchForJobs('default', function workerErrorFunc(payload, cb) {
      setTimeout(function() {
        cb(new Error('There was a big error'));
      }, payload.timeout);
    });

    Consumer.createJob('default', { timeout: 0 })
    .then(function(results) {
      done('This should have thrown an error.')
    })
    .catch(function(err) {
      expect(err).to.not.be.a('null');
      done();
    });
  });

  it('Work should be somwhat distributed amongst various workers', function(done) {

    var allConnected = _.after(4, function() {
      var representedServers = [];
      var numbersToProcess = _.range(1, 1000);

      var iAmDone = _.after(numbersToProcess.length, function() {
        expect(_.unique(representedServers)).to.have.length.above(1);
        done();
      });

      Worker1.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 1 });
      });
      Worker2.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 2 });
      });
      Worker3.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 3 });
      });
      Worker4.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 4 });
      });

      _.each(numbersToProcess, function(num) {
        (function(num) {
          Consumer.createJob('default', { input: num })
          .then(function(results) {
            representedServers.push(results.workerId);
            expect(results.result).to.equal(num * 2);
            iAmDone();
          });
        })(num);
      });
    });

    var Worker1 = new comrade.Worker('postgres://localhost/postgres', allConnected, 1);
    var Worker2 = new comrade.Worker('postgres://localhost/postgres', allConnected, 2);
    var Worker3 = new comrade.Worker('postgres://localhost/postgres', allConnected, 3);
    var Worker4 = new comrade.Worker('postgres://localhost/postgres', allConnected, 4);
  });

  it('Work done by different instances on different queues', function(done) {

    var allConnected = _.after(4, function() {

      var iAmDone = _.after(4, function() {
        done();
      });

      Worker1.watchForJobs('adder', function adder(payload, cb) {
        cb(null, { result: payload.val1 + payload.val2 });
      });
      Worker2.watchForJobs('subtractor', function subtractor(payload, cb) {
        cb(null, { result: payload.val1 - payload.val2 });
      });
      Worker3.watchForJobs('multiplier', function multiplier(payload, cb) {
        cb(null, { result: payload.val1 * payload.val2 });
      });
      Worker4.watchForJobs('divider', function divider(payload, cb) {
        cb(null, { result: payload.val1 / payload.val2 });
      });

      Consumer.createJob('adder', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(15);
        iAmDone();
      });
      Consumer.createJob('subtractor', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(5);
        iAmDone();
      });
      Consumer.createJob('multiplier', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(50);
        iAmDone();
      });
      Consumer.createJob('divider', { val1: 10, val2: 5 })
      .then(function(results) {
        expect(results.result).to.equal(2);
        iAmDone();
      });

    });

    var Worker1 = new comrade.Worker('postgres://localhost/postgres', allConnected, 1);
    var Worker2 = new comrade.Worker('postgres://localhost/postgres', allConnected, 2);
    var Worker3 = new comrade.Worker('postgres://localhost/postgres', allConnected, 3);
    var Worker4 = new comrade.Worker('postgres://localhost/postgres', allConnected, 4);
  });

});


