var expect  = require('chai').expect;
var comrade = require('../');
var _       = require('lodash');
var Consumer;
var Producer;

beforeEach(function(done) {
  var iAmDone = _.after(2, done);

  Consumer = new comrade.Consumer('postgres://localhost/postgres', function(err, client) {
    Consumer.deleteAllJobs(function(err, results) {
      iAmDone();
    });
  }, 0);

  Producer = new comrade.Producer('postgres://localhost/postgres', function(err, client) {
    iAmDone();
  }, 0);
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
      var numbersToProcess = _.range(1, 1000);

      var iAmDone = _.after(numbersToProcess.length, function() {
        expect(_.unique(representedServers)).to.have.length.above(1);
        done();
      });

      Consumer1.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 1 });
      });
      Consumer2.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 2 });
      });
      Consumer3.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 3 });
      });
      Consumer4.watchForJobs('default', function multiplierIdentifier(payload, cb) {
        cb(null, {result: payload.input * 2, workerId: 4 });
      });

      _.each(numbersToProcess, function(num) {
        (function(num) {
          Producer.createJob('default', { input: num })
          .then(function(results) {
            representedServers.push(results.workerId);
            expect(results.result).to.equal(num * 2);
            iAmDone();
          });
        })(num);
      });
    });

    var Consumer1 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 1, 0, 4);
    var Consumer2 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 2, 0, 4);
    var Consumer3 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 3, 0, 4);
    var Consumer4 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 4, 0, 4);
  });

  it('Work done by different instances on different queues', function(done) {

    var allConnected = _.after(4, function() {

      var iAmDone = _.after(4, function() {
        done();
      });

      Consumer1.watchForJobs('adder', function adder(payload, cb) {
        cb(null, { result: payload.val1 + payload.val2 }, { workerId: 'consumer1'});
      }, { workerId: 'consumer1' });
      Consumer2.watchForJobs('subtractor', function subtractor(payload, cb) {
        cb(null, { result: payload.val1 - payload.val2 }, { workerId: 'consumer2'});
      }, { workerId: 'consumer2' });
      Consumer3.watchForJobs('multiplier', function multiplier(payload, cb) {
        cb(null, { result: payload.val1 * payload.val2 }, { workerId: 'consumer3'});
      }, { workerId: 'consumer3' });
      Consumer4.watchForJobs('divider', function divider(payload, cb) {
        cb(null, { result: payload.val1 / payload.val2 }, { workerId: 'consumer4'});
      }, { workerId: 'consumer4' });

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

    var Consumer1 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 1);
    var Consumer2 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 2);
    var Consumer3 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 3);
    var Consumer4 = new comrade.Consumer('postgres://localhost/postgres', allConnected, 4);
  });

});


