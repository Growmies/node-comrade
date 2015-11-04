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
    Worker.watchForJobs(function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, 10);
    });

    Consumer.createJob({ input: 4})
    .then(function(results) {
      expect(results.result).to.equal(8);
      done();
    });

  });

  it('Should pick up multiple jobs', function(done) {
    Worker.watchForJobs(function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, payload.timeout);
    });

    var iAmDone = _.after(3, done);

    Consumer.createJob({ input: 4, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(8);
      iAmDone();
    });

    Consumer.createJob({ input: 5, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(10);
      iAmDone();
    });

    Consumer.createJob({ input: 6, timeout: 0 })
    .then(function(results) {
      expect(results.result).to.equal(12);
      iAmDone();
    });
  });

  it('Should properly handle errors', function(done) {
    Worker.watchForJobs(function workerErrorFunc(payload, cb) {
      setTimeout(function() {
        cb(new Error('There was a big error'));
      }, payload.timeout);
    });

    Consumer.createJob({ timeout: 0 })
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
      var iAmDone = _.after(6, function() {
        expect(_.unique(representedServers)).to.have.length.above(1);
        done();
      });

      Worker1.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 1 });
      });
      Worker2.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 2 });
      });
      Worker3.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 3 });
      });
      Worker4.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 4 });
      });

      // Create bunch of jobs now
      Consumer.createJob({ input: 0 })
      .then(function(results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(0);
        iAmDone();
      });

      Consumer.createJob({ input: 1 })
      .then(function(results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(2);
        iAmDone();
      });

      Consumer.createJob({ input: 2 })
      .then(function(results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(4);
        iAmDone();
      });

      Consumer.createJob({ input: 3 })
      .then(function(results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(6);
        iAmDone();
      });

      Consumer.createJob({ input: 4 })
      .then(function(results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(8);
        iAmDone();
      });

      Consumer.createJob({ input: 5 })
      .then(function(results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(10);
        iAmDone();
      });
    });

    var Worker1 = new comrade.Worker('postgres://localhost/postgres', allConnected, 1);
    var Worker2 = new comrade.Worker('postgres://localhost/postgres', allConnected, 2);
    var Worker3 = new comrade.Worker('postgres://localhost/postgres', allConnected, 3);
    var Worker4 = new comrade.Worker('postgres://localhost/postgres', allConnected, 4);
  });

});


