var cluster = require('cluster');
var _       = require('lodash');
var comrade = require('./');

if (cluster.isMaster) {
  var Consumer = new comrade.Consumer('postgres://localhost/postgres', function(err, client) {

    var numbersToProcess = _.range(1, 1000);
    var workerResults = [];
    var iAmDone = _.after(numbersToProcess.length, function() {
      console.log('All done!');
      console.log(_.countBy(workerResults));
    });

    _.each(numbersToProcess, function(num) {
      (function(num) {
        Consumer.createJob({ input: num })
        .then(function(results) {
          workerResults.push(results.worker);
          iAmDone();
        });
      })(num);
    });

    cluster.fork();
    cluster.fork();
    cluster.fork();
    cluster.fork();
    cluster.fork();
    cluster.fork();
    cluster.fork();
    cluster.fork();
    cluster.fork();
  });
  console.log('I am the master!');
}

if (cluster.isWorker) {
  var Worker = new comrade.Worker('postgres://localhost/postgres', function(err, client) {
    Worker.watchForJobs(function(payload, cb) {
      cb(null, { result: payload.input * 2, worker: cluster.worker.id });
    });
  }, cluster.worker.id);
}
