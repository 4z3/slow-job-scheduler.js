
transition_names = {};
transition_names[[undefined,'blocked']] = 'init';
transition_names[['blocked','running']] = '[32;1mwake up[m';
transition_names[['running','completed']] = '[31;1mcompleted[m';



Job = function (name, deps, duty) {
  this.name = name;
  this.deps = deps;
  this.duty = duty;
  this.restate('blocked');
};

Job.prototype.restate = function (state) {
  var transition = [this.state, state];
  var transition_name = transition_names[transition] ||
    '[35;1m' +
    transition.join(' -> ')
    + '[m'
    ;
  console.log(this.name, ':', transition_name);
  this.state = state;
};



Scheduler = function () {
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

Scheduler.prototype.run = function (callback) {

  if (callback) {
    this.add(Object.keys(this.jobs), Object.keys(this.jobs), callback);
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
        job.duty(function (result) {
          job.restate('completed');
          job.result = result;
          reschedule(turn);
        });
      });
  })(0);
};



function easy_task(callback) {
  //console.log('easy_task:', this);
  callback(42);
};

function hard_task(callback) {
  setTimeout(callback, 1000);
};

function broken_task(callback) {
};

scheduler = new Scheduler();

scheduler.add('J1', ['J2','J3', 'J5'], easy_task);
//scheduler.add('J1', ['J2','J3', 'J5'], broken_task);
scheduler.add('J2', ['J3','J4'], hard_task);
scheduler.add('J3', ['J4'], easy_task);
scheduler.add('J4', [], hard_task);
scheduler.add('J5', ['J4'], easy_task);

//if (0)
scheduler.run(function () {
  //console.log('done', pools);
  console.log('done:', this);
  //console.log('');
  //console.log('summary');
  //console.log('=======');
  //Object.keys(pools).forEach(function (name) {
  //  var pool = pools[name];
  //  console.log(name, 'jobs:', pool.length, pool.map(function (job) { return job.name }));
  //});
});

