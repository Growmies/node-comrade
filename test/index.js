var expect  = require('chai').expect;
var comrade = require('../');
var _       = require('lodash');
var Comrade;

beforeEach(function(done) {
  Comrade = new comrade.Comrade('postgres://localhost/postgres', function(err, client) {
    Comrade.deleteAllJobs(function(err, results) {
      done();
    });
  }, 0);
});

afterEach(function() {
  Comrade.destroy();
});

describe('Comrade job processor', function() {
  it('Should pick up a job', function(done) {
    Comrade.watchForJobs(function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, 10);
    });

    Comrade.createJob({ input: 4}, function(err, results) {
      expect(results.result).to.equal(8);
      done();
    });

  });

  it('Should pick up multiple jobs', function(done) {
    Comrade.watchForJobs(function multiplier(payload, cb) {
      setTimeout(function() {
        cb(null, {result: payload.input * 2});
      }, payload.timeout);
    });

    var iAmDone = _.after(3, done);

    Comrade.createJob({ input: 4, timeout: 0 }, function(err, results) {
      expect(results.result).to.equal(8);
      iAmDone();
    });

    Comrade.createJob({ input: 5, timeout: 0 }, function(err, results) {
      expect(results.result).to.equal(10);
      iAmDone();
    });

    Comrade.createJob({ input: 6, timeout: 0 }, function(err, results) {
      expect(results.result).to.equal(12);
      iAmDone();
    });
  });

  it('Should properly handle errors', function(done) {
    Comrade.watchForJobs(function workerErrorFunc(payload, cb) {
      setTimeout(function() {
        cb(new Error('There was a big error'));
      }, payload.timeout);
    });

    Comrade.createJob({ timeout: 0 }, function(err, results) {
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

      Comrade.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 0 });
      });
      Comrade1.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 1 });
      });
      Comrade2.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 2 });
      });
      Comrade3.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 3 });
      });
      Comrade4.watchForJobs(function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 4 });
      });

      // Create bunch of jobs now
      Comrade.createJob({ input: 0 }, function(err, results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(0);
        iAmDone();
      });

      Comrade.createJob({ input: 1 }, function(err, results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(2);
        iAmDone();
      });

      Comrade.createJob({ input: 2 }, function(err, results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(4);
        iAmDone();
      });

      Comrade.createJob({ input: 3 }, function(err, results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(6);
        iAmDone();
      });

      Comrade.createJob({ input: 4 }, function(err, results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(8);
        iAmDone();
      });

      Comrade.createJob({ input: 5 }, function(err, results) {
        representedServers.push(results.workerId);
        expect(results.result).to.equal(10);
        iAmDone();
      });
    });

    var Comrade1 = new comrade.Comrade('postgres://localhost/postgres', allConnected, 1);
    var Comrade2 = new comrade.Comrade('postgres://localhost/postgres', allConnected, 2);
    var Comrade3 = new comrade.Comrade('postgres://localhost/postgres', allConnected, 3);
    var Comrade4 = new comrade.Comrade('postgres://localhost/postgres', allConnected, 4);
  });

});


