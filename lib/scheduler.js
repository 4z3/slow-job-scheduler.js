var Job = require('./job').Job;

var Scheduler = exports.Scheduler = function () {
  this.jobs = {};
};

Scheduler.prototype.add = function (name, deps, duty) {
  var job = new Job(name, deps, duty);
  if (job.name in this.jobs) {
    throw new Error('job name conflict: ' + JSON.stringify(job.name));
  };
  this.jobs[job.name] = job;
  return this;
};

Scheduler.prototype.run = function (name, callback) {

  if (arguments.length === 1) {
    callback = name;
    name = JSON.stringify(Object.keys(this.jobs));
  };

  if (callback) {
    this.add(name, Object.keys(this.jobs), callback);
  };

  var jobs = this.jobs;
  this.jobs = {};

  // replace names by jobs in in all job deps
  Object.keys(jobs)
    .map(function (name) { return jobs[name] })
    .forEach(function (job) {
      job.deps = job.deps.map(function (name) {
        if (!(name in jobs)) {
          // TODO pass this to callback
          throw new Error('missing job ' + JSON.stringify(name)
            + ' required by ' + JSON.stringify(job.name));
        };
        return jobs[name]
      });
    });

  // {} -> []
  jobs = Object.keys(jobs).map(function (key) {
    return jobs[key];
  });

  //
  // The scheduler's main loop.
  //
  (function reschedule(turn) {
    jobs
      .filter(function (job) {
        // filter all blocked jobs where all dependecies are completed
        // they are now 'ready'
        if (job.state === 'blocked' &&
            job.deps
                .filter(function (job) {
                  return job.state === 'completed';
                })
                .length === job.deps.length) {
          job.restate('running');
          return true;
        };
      })
      .forEach(function (job) {
        var context = { self: job };
        job.deps.forEach(function (dep) {
            //context[dep.name] =  Object.create(dep.result || null);
            context[dep.name] = dep.result;
        });
        job.duty.call(context, function (result) {
          job.restate('completed');
          if (arguments.length > 0) {
            job.result = result;
          };
          reschedule(turn);
        });
      });
  })(0);
};

// schedule (w/ auto module loader
var schedule = exports.schedule = function (name) {
  var scheduler = new Scheduler();
  var scheduled_modules = {};
  var queue = name instanceof Array ? name.slice() : [name];
  var Module = require('./module');

  while (queue.length > 0) {
    name = queue.pop();
    if (!(name in scheduled_modules)) {
      if (!Module.has(name)) {
        Module.load(name);
      };
      if (Module.has(name)) {
        // TODO use modules[name] as prototype of module, so we can modify
        //      it [by adding revdeps] w/o mangling the original.
        var module = scheduled_modules[name] = Module.get(name);

        console.log('schedule module: ' + JSON.stringify(name));

        scheduler.add(module.name, module.deps, module.duty);
        module.deps.forEach(function (name) {
          queue.push(name);
        });
      } else {
        throw new Error('no such module: ' + JSON.stringify(name));
      };
    };
  };

  // add reverse dependencies to modules
  Object.keys(scheduled_modules).forEach(function (name) {
    var module = scheduled_modules[name];
    if ('revdeps' in module) {
      module.revdeps.forEach(function (revdep) {
        if (revdep in scheduled_modules) {
          var module = scheduled_modules[revdep];
          if (module.deps.indexOf(name) < 0) {
            console.log('revdep:', revdep, '<-', name);
            module.deps.push(name);
          };
        } else {
          console.log('revdep:', revdep, '<-', name, 'not. '
            , revdep, 'is no target');
        };
      });
    };
  });

  return scheduler;
};
