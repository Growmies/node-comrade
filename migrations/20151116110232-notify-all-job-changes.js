exports.up = function(db, callback) {
  var createJobChangeNotificationFunction = (
    ' CREATE OR REPLACE FUNCTION "jobChange"() ' +
    ' RETURNS trigger AS ' +
      ' $BODY$ ' +
        ' DECLARE ' +
          ' BEGIN ' +
            ' PERFORM pg_notify(\'jobChange\'::text, \'jobChange\'); ' +
            ' RETURN new; ' +
          ' END; ' +
      ' $BODY$ ' +
    ' LANGUAGE plpgsql VOLATILE '
  );

  var createJobChangeTrigger = (
    ' CREATE TRIGGER "jobChange" ' +
    ' AFTER INSERT OR UPDATE OR DELETE' +
    ' ON jobs ' +
    ' FOR EACH ROW ' +
    ' EXECUTE PROCEDURE "jobChange"(); '
  );

  db.runSql(createJobChangeNotificationFunction, function(err) {
    if (err) return callback(err);
    db.runSql(createJobChangeTrigger, function(err) {
      if (err) return callback(err);
      callback();
    });
  });
};

exports.down = function(db, callback) {
  var dropJobChangeTrigger  = 'DROP TRIGGER "jobChange" ON jobs';
  var dropJobChangeFunction = 'DROP FUNCTION "jobChange"()';

  db.runSql(dropJobChangeTrigger, function(err) {
    if (err) return callback(err);
    db.runSql(dropJobChangeFunction, function(err) {
      if (err) return callback(err);
      callback();
    });
  });

};
