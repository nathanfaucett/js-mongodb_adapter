var type = require("type"),
    each = require("each"),
    MongoClient = require('mongodb').MongoClient;


function MongoAdapter(options) {
    options || (options = {});

    this.db = null;
    this.database = options.database != null ? options.database : "test";
    this.port = type.isNumber(options.port) ? +options.port : 27017;

    this._counters = {};
}

MongoAdapter.prototype.init = function(callback) {
    var _this = this,
        collection = this._collection,
        schema = collection && collection._schema;

    MongoClient.connect("mongodb://127.0.0.1:" + this.port + "/" + this.database, function(err, db) {
        if (err) {
            callback(err);
            return;
        }
        _this.db = db;

        process.on("exit", function() {
            db.close();
        });

        if (schema) {
            createCallback = series(callback);

            each(schema.tables, function(tableSchema, tableName) {
                var counters = _this._counters[tableName] = {};

                each(tableSchema.columns, function(column, columnName) {
                    each(column, function(value, key) {
                        if (key === "autoIncrement") {
                            addAutoIncrement(counters, db, tableName, columnName, createCallback());
                        } else if (key === "unique") {
                            addUnique(db, tableName, columnName, createCallback());
                        }
                    });
                });
            });
            return;
        }

        callback();
    });
    return this;
};

MongoAdapter.prototype.close = function() {

    this.db.close();
    return this;
};

function addAutoIncrement(counters, db, tableName, columnName, callback) {
    var collectionName = createCollectionName(tableName, columnName);

    hasItem(db, collectionName, {
        _id: columnName
    }, function(has) {
        var collection = db.collection(collectionName);

        counters[columnName] = function(done) {
            collection.findAndModify({
                    _id: columnName
                },
                null, {
                    $inc: {
                        seq: 1
                    }
                }, function(err, value) {
                    if (err || !value) {
                        done(err);
                        return;
                    }
                    done(undefined, value.seq);
                });
        };

        if (!has) {
            collection.insert({
                _id: columnName,
                seq: 1
            }, function(err) {
                if (err) {
                    callback(err);
                    return;
                }
                callback();
            });
        } else {
            callback();
        }
    });
}

function addUnique(db, tableName, columnName, callback) {
    var value = {};

    value[columnName] = 1;

    db.collection(tableName).ensureIndex(value, {
        unique: true
    }, function(err, indexName) {
        if (err) {
            callback(err);
            return;
        }
        callback(undefined, indexName);
    });
}

function hasItem(db, collectionName, query, callback) {
    var collection = db.collection(collectionName);

    collection.findOne(query, function(err, doc) {
        if (err || !doc) {
            callback(false);
            return;
        }
        callback(true);
    });
}

function createCollectionName(tableName, columnName) {

    return "__" + tableName + "_" + columnName + "__";
}

function series(done) {
    var length = 0,
        called = false;

    function callback(err) {
        if (called === true) {
            return;
        }
        if (--length <= 0 || err) {
            called = true;
            done(err);
        }
    }

    return function createCallback() {
        length++;
        return callback;
    };
}

MongoAdapter.prototype.save = function(tableName, params, callback) {
    var collection = this.db.collection(tableName),
        counters = this._counters[tableName],
        createCallback;

    createCallback = series(function(err) {
        if (err) {
            callback(err);
            return;
        }

        collection.insert(params, function(err, row) {
            if (err) {
                callback(err);
                return;
            }

            callback(undefined, row[0]);
        });
    });

    each(counters, function(counter, columnName) {
        var next = createCallback();

        counter(function(err, value) {
            if (err) {
                next(err);
                return;
            }

            params[columnName] = value;
            next();
        });
    });

    return this;
};

MongoAdapter.prototype.update = function(tableName, id, params, callback) {
    var _this = this;

    this.db.collection(tableName).update({
        id: id
    }, {
        $set: params
    }, function(err) {
        if (err) {
            callback(err);
            return;
        }

        _this.findOne(tableName, {
            where: {
                id: id
            }
        }, callback);
    });
    return this;
};

MongoAdapter.prototype.find = function(tableName, query, callback) {

    this.db.collection(tableName).find(query.where || {}, function(err, docs) {
        if (err) {
            callback(err);
            return;
        }

        docs.toArray(function(err, items) {
            if (err) {
                callback(err);
                return;
            }
            callback(undefined, items);
        });
    });
    return this;
};

MongoAdapter.prototype.findOne = function(tableName, query, callback) {

    this.db.collection(tableName).findOne(query.where || {}, function(err, item) {
        if (err || !item) {
            callback(err || new Error("MongoAdapter.findOne(tableName, query, callback)"));
            return;
        }

        callback(undefined, item);
    });
    return this;
};

MongoAdapter.prototype.destroy = function(tableName, query, callback) {
    var collection = this.db.collection(tableName),
        where = query.where || {};

    collection.find(where, function(err, docs) {
        if (err) {
            callback(err);
            return;
        }

        docs.toArray(function(err, items) {
            if (err || !items.length) {
                callback(err);
                return;
            }

            collection.remove(where, function(err) {
                if (err) {
                    callback(err);
                    return;
                }

                callback(undefined, items);
            });
        });
    });
    return this;
};

MongoAdapter.prototype.createTable = function(tableName, columns, options, callback) {

    callback(new Error("createTable(tableName, columns, options, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.renameTable = function(tableName, newTableName, callback) {

    callback(new Error("renameTable(tableName, newTableName, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.removeTable = function(tableName, callback) {

    callback(new Error("removeTable(tableName, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.addColumn = function(tableName, columnName, column, options, callback) {

    callback(new Error("addColumn(tableName, columnName, column, options, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.renameColumn = function(tableName, columnName, newColumnName, callback) {

    callback(new Error("renameColumn(tableName, columnName, newColumnName, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.removeColumn = function(tableName, columnName, callback) {

    callback(new Error("removeColumn(tableName, columnName, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.addIndex = function(tableName, columnName, options, callback) {

    callback(new Error("addIndex(tableName, columnName, options, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.removeIndex = function(tableName, columnName, options, callback) {

    callback(new Error("removeIndex(tableName, columnName, options, callback) MongoAdapter not implemented"));
    return this;
};

MongoAdapter.prototype.removeDatabase = function(callback) {

    callback(new Error("removeDatabase(callback) MongoAdapter not implemented"));
    return this;
};


module.exports = MongoAdapter;
