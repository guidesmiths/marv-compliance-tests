var Hath = require('hath')
var async = require('async')

function withDatabase(t, done, fn) {
    var driver = t.locals.driver
    async.series([driver.connect, driver.dropMigrations, fn, driver.disconnect], function(err) {
        if (err) throw err
        done()
    })
}

function shouldCreateMigrationTableIfNotExists(t, done) {
    var driver = t.locals.driver
    withDatabase(t, done, function(cb) {
        async.series({
            meh1: driver.ensureMigrations,
            migrations: driver.getMigrations,
            meh2: driver.disconnect
        }, function(err, results) {
            if (err) throw err
            t.assert(results.migrations, 'Migrations created')
            t.assert(results.migrations.length === 0, 'Migrations is an array')
            cb()
        })
    })
}

function shouldNotFailIfMigrationTableAlreadyExists(t, done) {
    var driver = t.locals.driver
    withDatabase(t, done, function(cb) {
        async.series([
            driver.ensureMigrations,
            driver.ensureMigrations,
            driver.disconnect
        ], function(err) {
            if (err) throw err
            cb()
        })
    })
}

function shouldLockMigrationsTable(t, done) {
    var driver1 = t.locals.driver
    var driver2 = t.locals.driver2
    var delay
    withDatabase(t, done, function(cb) {
        async.series([
            driver1.ensureMigrations,
            driver1.lockMigrations,
            driver2.connect,
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
            driver1.disconnect,
            driver2.unlockMigrations,
            driver2.disconnect
        ], function(err) {
            if (err) throw err
            t.assert(delay >= 200, 'Delay >= 200')
            t.assert(delay <= 400, 'Delay <= 400')
            cb()
        })
    })
}

function shouldRunMigration(t, done) {
    var driver = t.locals.driver
    var migration = t.locals.migration
    withDatabase(t, done, function(cb) {
        async.waterfall([
            driver.ensureMigrations,
            driver.runMigration.bind(driver, migration),
            driver.getMigrations
        ], function(err, migrations) {
            if (err) throw err
            t.assert(migrations, 'Migrations created')
            t.assert(migrations.length === 1, 'Migrations table updated')
            t.assert(migrations[0].level === migration.level, 'Migration level')
            t.assert(migrations[0].comment === migration.comment, 'Migration comment')
            t.assert(migrations[0].timestamp.toISOString() === migration.timestamp.toISOString(), 'Migration timestamp')
            t.assert(migrations[0].checksum === migration.checksum, 'Migration checksum')
            cb()
        })
    })
}

function shouldSkipExistingMigration(t, done) {
    var driver = t.locals.driver
    var migration = t.locals.migration
    withDatabase(t, done, function(cb) {
        async.waterfall([
            driver.ensureMigrations,
            driver.runMigration.bind(driver, migration),
            driver.runMigration.bind(driver, migration),
            driver.getMigrations
        ], function(err, migrations) {
            if (err) throw err
            t.assert(migrations, 'Migrations created')
            t.assert(migrations.length === 1, 'Migrations table not updated')
            cb()
        })
    })
}


module.exports = Hath.suite('Compliance Tests', [
    shouldCreateMigrationTableIfNotExists,
    shouldNotFailIfMigrationTableAlreadyExists,
    shouldLockMigrationsTable,
    shouldRunMigration
])