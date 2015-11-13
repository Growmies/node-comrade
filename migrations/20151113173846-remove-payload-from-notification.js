var dbm = global.dbm || require('db-migrate');
var type = dbm.dataType;

exports.up = function(db, callback) {
  var removePayloadFromNotification = (
    ' CREATE OR REPLACE FUNCTION "newJob"() ' +
    ' RETURNS trigger AS ' +
      ' $BODY$ ' +
        ' DECLARE ' +
          ' BEGIN ' +
            ' PERFORM pg_notify(\'newJob\'::text , NEW."id" || \'::\' || NEW.queue); ' +
            ' RETURN new; ' +
          ' END; ' +
      ' $BODY$ ' +
    ' LANGUAGE plpgsql VOLATILE '
  );
  db.runSql(removePayloadFromNotification, callback);
};

exports.down = function(db, callback) {
  var putPayloadBackIn = (
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
  db.runSql(putPayloadBackIn, callback);
};
