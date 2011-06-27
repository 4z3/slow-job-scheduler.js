var Job = exports.Job = function (name, deps, duty) {
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

transition_names = {};
transition_names[[undefined,'blocked']] = 'init';
transition_names[['blocked','running']] = '[32;1mwake up[m';
transition_names[['running','completed']] = '[31;1mcompleted[m';
