const async = require('async');
const { Suite, Test, Hook } = require('zunit');
const { ok, strictEqual: eq } = require('assert');

const setupDatabase = new Hook('Setup database', (h, done) => {
  const driver1 = h.test.locals.get('driver1');
  const driver2 = h.test.locals.get('driver2');
  async.series([driver1.connect, driver2.connect, driver1.dropMigrations], done);
});

const teardownDatabase = new Hook('Teardown database', (h, done) => {
  const driver1 = h.test.locals.get('driver1');
  const driver2 = h.test.locals.get('driver2');
  async.series([driver1.disconnect, driver2.disconnect], done);
});

const shouldCreateMigrationsTableIfNotExists = new Test('should create migration table if not exists', (t, done) => {
  const driver1 = t.locals.get('driver1');
  async.series(
    {
      meh1: driver1.ensureMigrations,
      migrations: driver1.getMigrations,
    },
    (err, results) => {
      if (err) return done(err);
      ok(results.migrations, 'Migrations table was not created');
      eq(results.migrations.length, 0);
      done();
    }
  );
});

const shouldCreateMigrationsTableInParallel = new Test('should create migration table in parallel', (t, done) => {
  const driver1 = t.locals.get('driver1');
  const driver2 = t.locals.get('driver2');
  async.series(
    {
      meh1: async.parallel.bind(async, [driver1.ensureMigrations, driver2.ensureMigrations]),
      migrations: driver1.getMigrations,
    },
    (err, results) => {
      if (err) return done(err);
      ok(results.migrations, 'Migrations table was not created');
      eq(results.migrations.length, 0);
      done();
    }
  );
});

const shouldNotFailIfMigrationsTableAlreadyExists = new Test(
  'should not fail if migration table already exists',
  (t, done) => {
    const driver1 = t.locals.get('driver1');
    async.series([driver1.ensureMigrations, driver1.ensureMigrations], done);
  }
);

const shouldLockMigrationsTable = new Test('should lock migrations table', (t, done) => {
  const driver1 = t.locals.get('driver1');
  const driver2 = t.locals.get('driver2');
  let delay;
  async.series(
    [
      driver1.ensureMigrations,
      driver1.lockMigrations,
      (cb) => {
        delay = Date.now();
        cb();
      },
      (cb) => {
        setTimeout(() => {
          driver1.unlockMigrations(cb);
          delay = Date.now() - delay;
        }, 200);
      },
      driver2.lockMigrations,
      driver2.unlockMigrations,
    ],
    (err) => {
      if (err) return done(err);
      ok(delay >= 200);
      ok(delay <= 400);
      done();
    }
  );
});

const shouldRunMigration = new Test('should run migration', (t, done) => {
  const driver1 = t.locals.get('driver1');
  const migration = t.locals.get('migrations').simple;
  async.waterfall(
    [driver1.ensureMigrations, driver1.runMigration.bind(driver1, migration), driver1.getMigrations],
    (err, migrations) => {
      if (err) return done(err);
      ok(migrations, 'Migrations created');
      eq(migrations.length, 1);
      eq(migrations[0].level, migration.level);
      eq(migrations[0].comment, migration.comment);
      eq(migrations[0].timestamp.toISOString(), migration.timestamp.toISOString());
      eq(migrations[0].checksum, migration.checksum);
      done();
    }
  );
});

const shouldRerunRepeatableMigration = new Test('should rerun repeatable migration', (t, done) => {
  const driver1 = t.locals.get('driver1');
  const migration = t.locals.get('migrations').simple;
  async.waterfall(
    [
      driver1.ensureMigrations,
      driver1.runMigration.bind(driver1, {
        level: migration.level,
        script: migration.script,
        directives: { audit: false },
      }),
      driver1.runMigration.bind(driver1, {
        level: migration.level,
        script: migration.script,
        directives: { audit: false },
      }),
      driver1.getMigrations,
    ],
    (err, migrations) => {
      if (err) return done(err);
      ok(migrations, 'Migrations created');
      eq(migrations.length, 0);
      done();
    }
  );
});

const shouldRecordNamespace = new Test('should record namespace', (t, done) => {
  const driver1 = t.locals.get('driver1');
  const migration = t.locals.get('migrations').namespace;
  async.waterfall(
    [driver1.ensureMigrations, driver1.runMigration.bind(driver1, migration), driver1.getMigrations],
    (err, migrations) => {
      if (err) return done(err);
      ok(migrations, 'Migrations created');
      eq(migrations.length, 1);
      eq(migrations[0].level, migration.level);
      eq(migrations[0].comment, migration.comment);
      eq(migrations[0].timestamp.toISOString(), migration.timestamp.toISOString());
      eq(migrations[0].checksum, migration.checksum);
      eq(migrations[0].namespace, migration.namespace);
      done();
    }
  );
});

const shouldIsolateByNamespace = new Test('should isolate by namespace', (t, done) => {
  const driver1 = t.locals.get('driver1');
  const { simple } = t.locals.get('migrations');
  const namespaced = t.locals.get('migrations').namespace;
  async.waterfall(
    [
      driver1.ensureMigrations,
      driver1.runMigration.bind(driver1, simple),
      driver1.runMigration.bind(driver1, namespaced),
      driver1.getMigrations,
    ],
    (err, migrations) => {
      if (err) return done(err);
      ok(migrations, 'Migrations created');
      eq(migrations.length, 2);
      eq(migrations[0].namespace, 'default');
      eq(migrations[1].namespace, namespaced.namespace);
      done();
    }
  );
});

const shouldReportMigrationErrors = new Test('should report migration errors', (t, done) => {
  const driver1 = t.locals.get('driver1');
  const migration = t.locals.get('migrations').fail;
  async.waterfall([driver1.ensureMigrations, driver1.runMigration.bind(driver1, migration)], (err) => {
    ok(err);
    ok(err.migration);
    eq(err.migration.level, 5);
    eq(err.migration.script, 'INVALID');
    done();
  });
});

module.exports = new Suite('Compliance Tests')
  .beforeEach(setupDatabase)
  .add(shouldCreateMigrationsTableIfNotExists)
  .add(shouldCreateMigrationsTableInParallel)
  .add(shouldNotFailIfMigrationsTableAlreadyExists)
  .add(shouldLockMigrationsTable)
  .add(shouldRunMigration)
  .add(shouldRerunRepeatableMigration)
  .add(shouldRecordNamespace)
  .add(shouldIsolateByNamespace)
  .add(shouldReportMigrationErrors)
  .afterEach(teardownDatabase);
