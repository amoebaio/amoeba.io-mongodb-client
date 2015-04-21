var MongoClient = require('mongodb').MongoClient;
var _ = require('underscore');

MongodbClient = function(config) {
    if (typeof(config) == "string") {
        this.url = config;
    } else {
        this.url = config.url;
    }
    this.pathSkip = config.pathSkip || 1;
    this.amoeba = null;
    this.connection = null;
};

MongodbClient.prototype.init = function(amoeba, onadded) {
    var self = this;
    this.amoeba = amoeba.root();
    MongoClient.connect(this.url, function(err, db) {
        if (!err) {
            self.connection = db;
        }
        if (onadded) {
            onadded(err, db);
        }
    });
};

MongodbClient.prototype.invoke = function(context, next) {
    var params = context.request.arguments.slice();

    params.unshift(context.request.path);

    if (context.response) {
        params.push(function(err, result) {
            context.response.error = err;
            context.response.result = result;
            next();
        });
    }
    this[context.request.method].apply(this, params);
    if (!context.response) {
        next();
    }
};

MongodbClient.prototype.add = function(path, object, callback) {

    var used = path.split('.').splice(this.pathSkip).join('.');

    var self = this;
    try {
        this.connection.collection(used).insert(object, function(err, result) {
            if (err) {
                if (callback) callback(err);
            } else {
                if (callback) callback(err, result.ops);
                for (var i = 0; i < result.ops.length; i++) {
                    self.amoeba.root().emit("added", path, result.ops[i]);
                }
            }
        });
    } catch (err) {
        if (callback) callback(err);
    }
};

MongodbClient.prototype.get = function(path) {

    var callback = Array.prototype.pop.call(arguments);
    try {

        this.connection.collection(path.split('.').splice(this.pathSkip).join('.')).find(arguments[1] || {}, arguments[2] || {}).toArray(callback);
    } catch (err) {
        callback(err);
    }
};

MongodbClient.prototype.del = function(path) {
    if (typeof(arguments[arguments.length - 1]) == "function") {
        var callback = Array.prototype.pop.call(arguments);
    }
    var used = path.split('.').splice(this.pathSkip).join('.');

    var self = this;
    try {
        this.connection.collection(used).find(arguments[1] || {}, arguments[2] || {}).toArray(function(err, result) {
            if (err) {
                if (callback) callback(err);
                return;
            }

            var already = 0;
            var total = result.length;

            _.each(result, function(obj) {

                self.connection.collection(used).findOneAndDelete({
                    _id: obj._id
                }, {}, function(err, res) {

                    if (err) {
                        //FIX: what i need do if error here???
                    } else {
                        self.amoeba.root().emit('deleted', path + "." + res.value._id, res.value);
                    }
                    already++;
                    if (total == already) {
                        if (callback) callback(err, result);
                    }

                });

            });

        });
    } catch (err) {
        if (callback) callback(err);
    }

};


MongodbClient.prototype.set = function(path) {
    if (typeof(arguments[arguments.length - 1]) == "function") {
        var callback = Array.prototype.pop.call(arguments);
    }
    var used = path.split('.').splice(this.pathSkip).join('.');

    var self = this;
    var set = arguments[1];
    try {
        this.connection.collection(used).find(arguments[2] || {}, arguments[3] || {}).toArray(function(err, result) {

            if (err) {
                if (callback) callback(err);
                return;
            }
            var modified = 0;
            var processed = 0;
            var total = result.length;

            _.each(result, function(obj) {
                var before = obj;
                self.connection.collection(used).findOneAndUpdate({
                    _id: obj._id
                }, {
                    $set: set
                }, {
                    upsert: false,
                    returnOriginal: false
                }, function(err, res) {
                    var diff = _.omit(arguments[1].value, function(v, k) {
                        return _.isEqual(before[k], v);
                    });
                    if (!_.isEmpty(diff)) {
                        self.amoeba.root().emit('updated', path + "." + obj._id, diff);
                        modified++;
                    }
                    processed++;
                    if (processed == total) {
                        if (callback) callback(null, {
                            "modified": modified
                        });
                    }
                });
            });

        });
    } catch (err) {
        if (callback) callback(err);
    }
};

module.exports = exports = MongodbClient;
