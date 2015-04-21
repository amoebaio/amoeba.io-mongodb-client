var assert = require("assert");
var Amoeba = require("../../amoeba.io");
var MongodbClient = require("../lib/amoeba-mongodb-client");
var MongoClient = require('mongodb').MongoClient;

var connectionUrl = "mongodb://localhost:27017/";

var amoeba = new Amoeba();
var connection = null;

/**
 * Before test you need to create database amoeba-test and check connection
 */
describe('AmoebaMongoDBClient', function() {

    var database = "amoeba-test";

    var prefix = "mongodb";

    var test_obj = {
        foo: 4,
        bar: "barbar"
    };

    before(function(done) {
        amoeba.path(prefix + ".*").as(new MongodbClient({
            url: connectionUrl
        }), function(err, added) {
            if (err) throw err;
            MongoClient.connect(connectionUrl, function(err, db) {
                if (err) throw err;
                connection = db;
                done();
            });
        });
    });

    beforeEach(function(done) {
        connection.collection('data').remove({});
        done();
    });

    it('#constructor', function(done) {
        amoeba.path("mongodb-mock.*").as(new MongodbClient("mongodb://l:34"), function(err) {
            assert.ok(err);
            done();
        });
    });
    it('#add one record without callback', function(done) {
        amoeba.path(prefix + ".data").invoke("add", {
            "x": 5,
            "y": 14
        });
        connection.collection("data").find({}).toArray(function(err, docs) {
            assert.equal(docs.length, 1);
            assert.equal(docs[0].x, 5);
            done();
        });
    });

    it('#add one record callback', function(done) {
        amoeba.path(prefix + ".data").invoke("add", {
            "x": 5,
            "y": 14
        }, function(err, result) {
            assert.equal(err, null);
            assert.equal(result[0].x, 5);
            done();
        });
    });

    it('#add one record strict event', function(done) {
        amoeba.path(prefix + ".data").on("added", function(data, event, path) {
            assert.equal(path, prefix + ".data");
            assert.equal(event, "added");
            assert.equal(data.x, 5);
            amoeba.removeAllListeners();
            done();
        });
        amoeba.path(prefix + ".data").invoke("add", {
            "x": 5,
            "y": 14
        });
    });

    it('#add one record event path mask test', function(done) {
        amoeba.path(prefix + ".*").on("added", function(data, event, path) {
            assert.equal(path, prefix + ".data");
            assert.equal(event, "added");
            assert.equal(data.x, 7);
            amoeba.removeAllListeners();
            done();
        });
        amoeba.path(prefix + ".data").invoke("add", {
            "x": 7,
            "y": 14
        });
    });

    it('#add one record event  mask test', function(done) {
        amoeba.path(prefix + ".*").on("*", function(data, event, path) {
            assert.equal(path, prefix + ".data");
            assert.equal(event, "added");
            assert.equal(data.x, 7);
            amoeba.removeAllListeners();
            done();
        });
        amoeba.path(prefix + ".data").invoke("add", {
            "x": 7,
            "y": 14
        });
    });

    it('#add many record', function(done) {
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 5,
            "y": 14
        }, {
            "x": 6,
            "y": 8
        }], function(err, result) {
            assert.ok(err === null);
            assert.ok(result[0]._id !== null);
            assert.equal(result[0].x, 5);
            assert.equal(result[0].y, 14);
            assert.ok(result[1]._id !== null);
            assert.equal(result[1].x, 6);
            assert.equal(result[1].y, 8);
            done();
        });
    });

    it('#add many record event test', function(done) {
        var objs = [{
            "x": 5,
            "y": 14
        }, {
            "x": 6,
            "y": 8
        }];
        var total = 0;
        amoeba.path(prefix + ".*").on("added", function(data, event, path) {
            assert.equal(event, 'added');
            assert.equal(path, prefix + ".data");
            assert.equal(data.x, objs[total].x);
            total++;
            if (total === 2) {
                amoeba.removeAllListeners();
                done();
            }
        });

        amoeba.path(prefix + ".data").invoke("add", objs);
    });

    it('#get one record', function(done) {
        amoeba.path(prefix + ".data").invoke("add", {
            "x": 5,
            "y": 14
        }, function(err, result) {

            amoeba.path(prefix + ".data").invoke("get", {}, function(err, result) {
                assert.equal(result[0].x, 5);
                assert.equal(result[0].y, 14);
                assert.ok(result[0]._id !== null);
                done();
            });
        });
    });

    it('#get record with condition', function(done) {
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 5,
            "y": 14
        }, {
            "x": 6,
            "y": 15
        }], function(err, result) {
            amoeba.path(prefix + ".data").invoke("get", {
                x: 5
            }, {}, function(err, result) {
                assert.equal(result.length, 1);
                assert.equal(result[0].x, 5);
                assert.equal(result[0].y, 14);
                assert.ok(result[0]._id !== null);
                done();
            });
        });
    });

    it('#get record with options', function(done) {
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 1,
            "y": 'c'
        }, {
            "x": 2,
            "y": 'b'
        }, {
            "x": 3,
            "y": 'a'
        }], function(err, result) {
            amoeba.path(prefix + ".data").invoke("get", {}, {
                sort: 'y',
                limit: 2
            }, function(err, result) {
                assert.equal(result.length, 2);
                assert.equal(result[0].x, 3);
                assert.equal(result[1].x, 2);
                done();
            });
        });
    });

    it('#delete one record', function(done) {
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 5,
            "y": 14
        }, {
            "x": 6,
            "y": 8
        }], function(err, result) {
            amoeba.path(prefix + ".data").invoke("del", {
                _id: result[0]._id
            }, function(err, result) {

                amoeba.path(prefix + ".data").invoke("get", function(err, result) {
                    assert.equal(result.length, 1);
                    assert.equal(result[0].x, 6);
                    done();
                });
            });
        });
    });

    it('#delete one record event test', function(done) {
        var id_to_del = null;
        amoeba.path(prefix + ".data.*").on("deleted", function(data, event, path) {
            assert.equal(event, 'deleted');
            assert.equal(path, prefix + ".data." + id_to_del);
            assert.equal(data.x, 5);
            amoeba.removeAllListeners();
            done();
        });
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 5,
            "y": 14
        }, {
            "x": 6,
            "y": 8
        }], function(err, result) {
            id_to_del = result[0]._id;
            amoeba.path(prefix + ".data").invoke("del", {
                _id: result[0]._id
            });
        });
    });

    it('#delete record by where', function(done) {
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 5,
            "y": 14
        }, {
            "x": 11,
            "y": 8
        }, {
            "x": 10,
            "y": 9
        }], function(err, result) {
            amoeba.path(prefix + ".data").invoke("del", {
                "x": {
                    $gt: 5
                }
            }, function(err, result) {
                assert.equal(result.length, 2);
                amoeba.path(prefix + ".data").invoke("get", function(err, result) {
                    assert.equal(result.length, 1);
                    assert.equal(result[0].x, 5);
                    done();
                });
            });
        });
    });

    it('#delete record by where event', function(done) {
        var total = 0;
        var objs = [{
            "_id": 1,
            "x": 5,
            "y": 14
        }, {
            "_id": 2,
            "x": 11,
            "y": 8
        }, {
            "_id": 3,
            "x": 10,
            "y": 9
        }];
        amoeba.path(prefix + ".data.1").on("deleted", function(data, event, path) {
            assert.ok(false);
        });

        amoeba.path(prefix + ".data.*").on("deleted", function(data, event, path) {
            assert.equal(event, 'deleted');
            total++;
            if (total === 2) {
                amoeba.removeAllListeners();
                done();
            }
        });

        amoeba.path(prefix + ".data").invoke("add", objs, function(err, result) {
            amoeba.path(prefix + ".data").invoke("del", {
                "x": {
                    $gt: 5
                }
            });
        });
    });

    it('#set one record', function(done) {
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 2,
            "z": [1, 2, 3]
        }], function(err, result) {
            amoeba.path(prefix + ".data").invoke("set", {
                "x": 2,
                "y": 14,
                "z": [1, 3]
            }, function(err, result) {
                assert.equal(arguments[1].modified, 1);
                done();
            });
        });
    });

    it('#set one record event test', function(done) {

        amoeba.path(prefix + ".data.*").on("updated", function(data, event, path) {
            assert.deepEqual({
                x: 2
            }, data);
            assert.equal(event, "updated");
            amoeba.removeAllListeners();
            done();
        });

        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 5,
            "y": 14
        }], function(err, result) {
            amoeba.path(prefix + ".data").invoke("set", {
                "x": 2
            });
        });
    });

    it('#set many record', function(done) {
        amoeba.path(prefix + ".data").invoke("add", [{
            "x": 5,
            "y": 14
        }, {
            "x": 6,
            "y": 15
        }], function(err, result) {
            amoeba.path(prefix + ".data").invoke("set", {
                "x": 2
            }, function(err, result) {
                amoeba.path(prefix + ".data").invoke("get", function(err, result) {
                    assert.equal(result.length, 2);
                    assert.equal(result[0].x, 2);
                    assert.equal(result[0].y, 14);
                    assert.equal(result[1].x, 2);
                    assert.equal(result[1].y, 15);
                    done();
                });
            });
        });
    });

    it('#set many record event test', function(done) {
        var total = 0;
        var objs = [{
            "_id": 1,
            "x": 3,
            "y": 14
        }, {
            "_id": 2,
            "x": 6,
            "y": 15
        }];

        amoeba.path(prefix + ".data.*").on("updated", function(data, event, path) {
            assert.equal(data.x, 2);
            assert.equal(path, prefix + ".data." + objs[total]._id);
            total++;
            if (total === 2) {
                amoeba.removeAllListeners();
                done();
            }
        });

        amoeba.path(prefix + ".data").invoke("add", objs, function(err, result) {

            amoeba.path(prefix + ".data").invoke("set", {
                "x": 2
            });
        });
    });


    it('#set only needed updated', function(done) {
        var total = 0;
        var objs = [{
            "_id": 1,
            "x": 2,
            "y": 14
        }, {
            "_id": 2,
            "x": 6,
            "y": 15
        }];

        amoeba.path(prefix + ".data.*").on("updated", function(data, event, path) {
            assert.equal(path, prefix + ".data.2");
            done();
        });

        amoeba.path(prefix + ".data").invoke("add", objs, function(err, result) {

            amoeba.path(prefix + ".data").invoke("set", {
                "x": 2
            }, function(err, data) {
                assert.equal(data.modified, 1);
            });
        });
    });
});
