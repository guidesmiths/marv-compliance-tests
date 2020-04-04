var Hath = require('hath')
var async = require('async')
require('hath-assert')(Hath)

function withDatabase(t, done, fn) {
    var driver1 = t.locals.driver
    var driver2 = t.locals.driver2
    async.series([driver1.connect, driver2.connect, driver1.dropMigrations, fn, driver1.disconnect, driver2.disconnect], function(err) {
        if (err) return done(err)
        done()
    })
}

function shouldCreateMigrationsTableIfNotExists(t, done) {
    t.label('should create migration table if not exists')
    var driver = t.locals.driver
    withDatabase(t, done, function(cb) {
        async.series({
            meh1: driver.ensureMigrations,
            migrations: driver.getMigrations
        }, function(err, results) {
            if (err) return done(err)
            t.assertTruthy(results.migrations, 'Migrations table was not created')
            t.assertEquals(results.migrations.length, 0)
            cb()
        })
    })
}

function shouldCreateMigrationsTableInParallel(t, done) {
    t.label('should create migration table in parallel')
    var driver = t.locals.driver
    var driver2 = t.locals.driver2
    withDatabase(t, done, function(cb) {
        async.series({
            meh1: async.parallel.bind(async, [driver.ensureMigrations, driver2.ensureMigrations]),
            migrations: driver.getMigrations
        }, function(err, results) {
            if (err) return done(err)
            t.assertTruthy(results.migrations, 'Migrations table was not created')
            t.assertEquals(results.migrations.length, 0)
            cb()
        })
    })
}

function shouldNotFailIfMigrationsTableAlreadyExists(t, done) {
    t.label('should not fail if migration table already exists')
    var driver = t.locals.driver
    withDatabase(t, done, function(cb) {
        async.series([
            driver.ensureMigrations,
            driver.ensureMigrations
        ], cb)
    })
}

function shouldLockMigrationsTable(t, done) {
    t.label('should lock migrations table')
    var driver1 = t.locals.driver
    var driver2 = t.locals.driver2
    var delay
    withDatabase(t, done, function(cb) {
        async.series([
            driver1.ensureMigrations,
            driver1.lockMigrations,
            function(cb) {
                delay = Date.now()
                cb()
            },
            function(cb) {
                setTimeout(function() {
                    driver1.unlockMigrations(cb)
                    delay = Date.now() - delay
                }, 200)
            },
            driver2.lockMigrations,
            driver2.unlockMigrations
        ], function(err) {
            if (err) return cb(err)
            t.assertNotLess(delay, 199)
            t.assertNotGreater(delay, 400)
            cb()
        })
    })
}

function shouldRunMigration(t, done) {
    t.label('should run migration')
    var driver = t.locals.driver
    var migration = t.locals.migrations.simple
    withDatabase(t, done, function(cb) {
        async.waterfall([
            driver.ensureMigrations,
            driver.runMigration.bind(driver, migration),
            driver.getMigrations
        ], function(err, migrations) {
            if (err) return cb(err)
            t.assertTruthy(migrations, 'Migrations created')
            t.assertEquals(migrations.length, 1)
            t.assertEquals(migrations[0].level, migration.level)
            t.assertEquals(migrations[0].comment, migration.comment)
            t.assertEquals(migrations[0].timestamp.toISOString(), migration.timestamp.toISOString())
            t.assertEquals(migrations[0].checksum, migration.checksum)
            cb()
        })
    })
}

function shouldRerunRepeatableMigration(t, done) {
    t.label('should rerun repeatable migration')
    var driver = t.locals.driver
    var migration = t.locals.migrations.simple
    withDatabase(t, done, function(cb) {
        async.waterfall([
            driver.ensureMigrations,
            driver.runMigration.bind(driver, { level: migration.level, script: migration.script, directives: { audit: false } }),
            driver.runMigration.bind(driver, { level: migration.level, script: migration.script, directives: { audit: false } }),
            driver.getMigrations
        ], function(err, migrations) {
            if (err) return cb(err)
            t.assertTruthy(migrations, 'Migrations created')
            t.assertEquals(migrations.length, 0)
            cb()
        })
    })
}

function shouldRecordNamespace(t, done) {
    t.label('should record namespace')
    var driver = t.locals.driver
    var migration = t.locals.migrations.namespace
    withDatabase(t, done, function(cb) {
        async.waterfall([
            driver.ensureMigrations,
            driver.runMigration.bind(driver, migration),
            driver.getMigrations
        ], function(err, migrations) {
            if (err) return cb(err)
            t.assertTruthy(migrations, 'Migrations created')
            t.assertEquals(migrations.length, 1)
            t.assertEquals(migrations[0].level, migration.level)
            t.assertEquals(migrations[0].comment, migration.comment)
            t.assertEquals(migrations[0].timestamp.toISOString(), migration.timestamp.toISOString())
            t.assertEquals(migrations[0].checksum, migration.checksum)
            t.assertEquals(migrations[0].namespace, migration.namespace)
            cb()
        })
    })
}

function shouldIsolateByNamespace(t, done) {
    t.label('should isolate by namespace')
    var driver = t.locals.driver
    var simple =  t.locals.migrations.simple
    var namespaced = t.locals.migrations.namespace
    withDatabase(t, done, function(cb) {
        async.waterfall([
            driver.ensureMigrations,
            driver.runMigration.bind(driver, simple),
            driver.runMigration.bind(driver, namespaced),
            driver.getMigrations
        ], function(err, migrations) {
            if (err) return cb(err)
            t.assertTruthy(migrations, 'Migrations created')
            t.assertEquals(migrations.length, 2)
            t.assertEquals(migrations[0].namespace, 'default')
            t.assertEquals(migrations[1].namespace, namespaced.namespace)
            cb()
        })
    })
}

function shouldReportMigrationErrors(t, done) {
    t.label('should report migration errors')
    var driver = t.locals.driver
    var migration = t.locals.migrations.fail
    withDatabase(t, done, function(cb) {
        async.waterfall([
            driver.ensureMigrations,
            driver.runMigration.bind(driver, migration)
        ], function(err) {
            t.assertTruthy(err)
            t.assertTruthy(err.migration)
            t.assertEquals(err.migration.level, 5)
            t.assertEquals(err.migration.script, 'INVALID')
            cb()
        })
    })
}


module.exports = Hath.suite('Compliance Tests', [
    shouldCreateMigrationsTableIfNotExists,
    shouldCreateMigrationsTableInParallel,
    shouldNotFailIfMigrationsTableAlreadyExists,
    shouldLockMigrationsTable,
    shouldRunMigration,
    shouldRerunRepeatableMigration,
    shouldRecordNamespace,
    shouldIsolateByNamespace,
    shouldReportMigrationErrors
])
