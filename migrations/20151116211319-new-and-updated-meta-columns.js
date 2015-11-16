exports.up = function(db, callback) {
  db.runSql('ALTER TABLE jobs ADD COLUMN "workerMeta" jsonb;', function(err) {
    if (err) return callback(err);
    db.runSql('ALTER TABLE jobs RENAME COLUMN "meta" TO "resultsMeta"', callback);
  });
};

exports.down = function(db, callback) {
  db.runSql('ALTER TABLE jobs RENAME COLUMN "resultsMeta" TO "meta"', function(err) {
    if (err) return callback(err);
    db.runSql('ALTER TABLE jobs DROP COLUMN "workerMeta"', callback);
  });
};
