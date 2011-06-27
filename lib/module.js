var modules = {};

exports.load = function (name) {
  console.log('load module:', JSON.stringify(name));
  try {
    var module = modules[name] = require('../modules/' + name);
    if (!module.deps) {
      module.deps = [];
    };
    module.name = name;
  } catch (exn) {
    console.error(exn.stack);
  };
};

exports.get = function (name) {
  return modules[name];
};

exports.has = function (name) {
  return name in modules;
};
