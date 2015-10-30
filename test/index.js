var expect               = require('chai').expect;
var comrade              = require('../');
var Comrade;

beforeEach(function(done) {
  Comrade = new comrade.Comrade('postgres://localhost/postgres', function(err, client) {
    Comrade.deleteAllJobs(function(err, results) {
      done();
    });
  });
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

    Comrade.createJob({ input: 4, timeout: 0 }, function(err, results) {
      expect(results.result).to.equal(8);
    });

    Comrade.createJob({ input: 5, timeout: 0 }, function(err, results) {
      expect(results.result).to.equal(10);
    });

    Comrade.createJob({ input: 6, timeout: 0 }, function(err, results) {
      expect(results.result).to.equal(12);
      done();
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
});


