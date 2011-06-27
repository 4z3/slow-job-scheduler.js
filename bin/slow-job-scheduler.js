#! /usr/bin/env node

var Scheduler = require('../lib/scheduler');
var name = 'slow-job-scheduler';

Scheduler.schedule(process.argv.slice(2)).run(name, function (finish) {
  return finish();
});
