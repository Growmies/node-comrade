exports.up = function(db, callback) {
  var createJobsTable = (
    'CREATE TABLE jobs ' +
      '( ' +
        'id serial NOT NULL, ' +
        'queue text, ' +
        'payload jsonb, ' +
        'status text, ' +
        'result jsonb, ' +
        'meta jsonb, ' +
        '"insertionTime" timestamp with time zone, ' +
        '"processingStartTime" timestamp with time zone, ' +
        '"processingEndTime" timestamp with time zone, ' +
        'CONSTRAINT jobs_pkey PRIMARY KEY (id) ' +
      ') ' +
      'WITH ( ' +
        'OIDS=FALSE ' +
      '); '
  );

  var createNewJobNotificationFunction = (
    ' CREATE OR REPLACE FUNCTION "newJob"() ' +
    ' RETURNS trigger AS ' +
      ' $BODY$ ' +
        ' DECLARE ' +
          ' BEGIN ' +
            ' PERFORM pg_notify(\'newJob\'::text , NEW."id" || \'::\' || NEW.queue || \'::\' || NEW."payload"); ' +
            ' RETURN new; ' +
          ' END; ' +
      ' $BODY$ ' +
    ' LANGUAGE plpgsql VOLATILE '
  );

  var createJobCompleteNotificationFunction = (
    ' CREATE OR REPLACE FUNCTION "jobComplete"() ' +
    ' RETURNS trigger AS ' +
      ' $BODY$ ' +
        ' DECLARE ' +
          ' BEGIN ' +
            ' PERFORM pg_notify(\'jobComplete\'::text, NEW."id" || \'::\' || NEW.status); ' +
            ' RETURN new; ' +
          ' END; ' +
      ' $BODY$ ' +
    ' LANGUAGE plpgsql VOLATILE '
  );

  var createNewJobTrigger = (
    ' CREATE TRIGGER "newJob" ' +
    ' AFTER INSERT ' +
    ' ON jobs ' +
    ' FOR EACH ROW ' +
    ' EXECUTE PROCEDURE "newJob"(); '
  );

  var createJobCompleteTrigger = (
    ' CREATE TRIGGER "jobComplete" ' +
    ' AFTER UPDATE ' +
    ' ON jobs ' +
    ' FOR EACH ROW ' +
    ' WHEN ((new.status = ANY (ARRAY[\'done\'::text, \'error\'::text]))) ' +
    ' EXECUTE PROCEDURE "jobComplete"();'
  );

  db.runSql(createJobsTable, function(err) {
    if (err) return callback(err);
    db.runSql(createNewJobNotificationFunction, function(err) {
      if (err) return callback(err);
      db.runSql(createJobCompleteNotificationFunction, function(err) {
        if (err) return callback(err);
        db.runSql(createNewJobTrigger, function(err) {
          if (err) return callback(err);
          db.runSql(createJobCompleteTrigger, function(err) {
            if (err) return callback(err);
            callback();
          });
        });
      });
    });
  });

};

exports.down = function(db, callback) {
  var dropJobCompleteTrigger              = 'DROP TRIGGER "jobComplete" ON jobs';
  var dropNewJobTrigger                   = 'DROP TRIGGER "newJob" ON jobs';
  var dropJobCompleteNotificationFunction = 'DROP FUNCTION "jobComplete"();';
  var dropNewJobNotificationFunction      = 'DROP FUNCTION "newJob"();';
  var dropJobsTable                       = 'DROP TABLE "jobs"';

  db.runSql(dropJobCompleteTrigger, function(err) {
    if (err) return callback(err);
    db.runSql(dropNewJobTrigger, function(err) {
      if (err) return callback(err);
      db.runSql(dropJobCompleteNotificationFunction, function(err) {
        if (err) return callback(err);
        db.runSql(dropNewJobNotificationFunction, function(err) {
          if (err) return callback(err);
          db.runSql(dropJobsTable, function(err) {
            if (err) return callback(err);
            callback();
          });
        });
      });
    });
  });
};

