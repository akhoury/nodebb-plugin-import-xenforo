
var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-xenforo]';

(function(Exporter) {

    Exporter.setup = function(config, callback) {
        Exporter.log('setup');

        // mysql db only config
        // extract them from the configs passed by the nodebb-plugin-import adapter
        var _config = {
            host: config.dbhost || config.host || 'localhost',
            user: config.dbuser || config.user || 'user',
            password: config.dbpass || config.pass || config.password || undefined,
            port: config.dbport || config.port || 3306,
            database: config.dbname || config.name || config.database || 'xf'
        };

        Exporter.config(_config);
        Exporter.config('prefix', config.prefix || config.tablePrefix || '');

        config.custom = config.custom || {};
        if (typeof config.custom === 'string') {
            try {
                config.custom = JSON.parse(config.custom)
            } catch (e) {}
        }

        Exporter.config('custom', config.custom || {});

        Exporter.connection = mysql.createConnection(_config);
        Exporter.connection.connect();

        callback(null, Exporter.config());
    };

    Exporter.query = function(query, callback) {
        if (!Exporter.connection) {
            var err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }
        // console.log('\n\n====QUERY====\n\n' + query + '\n');
        Exporter.connection.query(query, function(err, rows) {
            //if (rows) {
            //    console.log('returned: ' + rows.length + ' results');
            //}
            callback(err, rows)
        });
    };

    Exporter.countUsers = function (callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix') || '';
        var query = 'SELECT count(*) '

            + 'FROM ' + prefix + 'user '
            + 'LEFT JOIN ' + prefix + 'user_profile ON ' + prefix + 'user_profile.user_id=' + prefix + 'user.user_id ';

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                callback(null, rows[0]['count(*)']);
            });
    };

    Exporter.getUsers = function(callback) {
        return Exporter.getPaginatedUsers(0, -1, callback);
    };
    Exporter.getPaginatedUsers = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix') || '';
        var startms = +new Date();

        var query = 'SELECT '
            + prefix + 'user.user_id as _uid, '
            + prefix + 'user.email as _email, '

            + prefix + 'user.is_banned as _banned, '
            + prefix + 'user.like_count as _reputation, '

            + prefix + 'user_profile.signature as _signature, '
            + prefix + 'user_profile.homepage as _website, '
            + prefix + 'user_profile._location as _location, '

            + prefix + 'user.register_date as _joindate, '
            + prefix + 'user.last_activity as _lastonline, '
            + prefix + 'user.user_state as _state, '
            + prefix + 'user.is_admin as _xf_is_admin, '
            + prefix + 'user.is_moderator as _xf_is_moderator, '

            + prefix + 'user_profile.dob_day as _xf_dob_day, '
            + prefix + 'user_profile.dob_month as _xf_dob_month, '
            + prefix + 'user_profile.dob_year as _xf_dob_year, '

            + 'FROM ' + prefix + 'user '
            + 'LEFT JOIN ' + prefix + 'user_profile ON ' + prefix + 'user_profile.user_id=' + prefix + 'user.user_id '

            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    // nbb forces signatures to be less than 150 chars
                    // keeping it HTML see https://github.com/akhoury/nodebb-plugin-import#markdown-note
                    row._signature = Exporter.truncateStr(row._signature || '', 150);

                    // from unix timestamp (s) to JS timestamp (ms)
                    row._joindate = ((row._joindate || 0) * 1000) || startms;
                    row._lastonline = ((row._lastonline || 0) * 1000) || startms;

                    // lower case the email for consistency
                    row._email = (row._email || '').toLowerCase();
                    row._website = Exporter.validateUrl(row._website);

                    row._level = row._xf_is_admin ? "administrator" : row._xf_is_moderator ? "moderator" : "";

                    if (row._xf_dob_day && row._xf_dob_month && row._xf_dob_year) {
                        row._birthday = "" + row._xf_dob_month + "/" + row._xf_dob_day + "/" + row._xf_dob_year;
                    }

                    map[row._uid] = row;
                });

                callback(null, map);
            });
    };

    var getConversations = function(callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        if (Exporter._conversationsMap) {
            return callback(null, Exporter._conversationsMap);
        }

        var prefix = Exporter.config('prefix');

        var query = 'SELECT '
            + prefix + 'conversation_master.conversation_id as _cvid, '
            + prefix + 'conversation_master.user_id as _uid1, '
            + prefix + 'conversation_recipient.user_id as _uid2 '
            + 'FROM ' + prefix + 'conversation_master '
            + 'LEFT JOIN ' + prefix + 'conversation_recipient ON ' + prefix + 'conversation_master.conversation_id = ' + prefix + 'conversation_recipient.conversation_id '
            + 'AND ' + prefix +'conversation_master.user_id != ' + prefix + 'conversation_recipient.user_id';

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                var map = {};
                rows.forEach(function(row) {
                    map[row._cvid] = row;
                });
                Exporter._conversationsMap = map;
                callback(null, map);
            });
    };

    Exporter.countMessages = function(callback) {
        callback = !_.isFunction(callback) ? noop : callback;
        var prefix = Exporter.config('prefix');

        var query = 'SELECT count(*) '
            + 'FROM ' + prefix + 'conversation_message ';

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                callback(null, rows[0]['count(*)']);
            });
    };

    Exporter.getMessages = function(callback) {
        return Exporter.getPaginatedMessages(0, -1, callback);
    };

    Exporter.getPaginatedMessages = function(start, limit, callback) {

        callback = !_.isFunction(callback) ? noop : callback;

        var startms = +new Date();
        var prefix = Exporter.config('prefix') || '';
        var query = 'SELECT '
            + prefix + 'conversation_message.message_id as _mid, '
            + prefix + 'conversation_message.conversation_id as _cvid, '
            + prefix + 'conversation_message.message_date as _timestamp, '
            + prefix + 'conversation_message.user_id as _fromuid, '
            + prefix + 'conversation_message.message as _content '

            + 'FROM ' + prefix + 'conversation_message '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        getConversations(function(err, conversationsMap) {
            Exporter.query(query,
                function(err, rows) {
                    if (err) {
                        Exporter.error(err);
                        return callback(err);
                    }
                    var map = {};
                    rows.forEach(function(row) {
                        row._timestamp = ((row._timestamp || 0) * 1000) || startms;

                        var conversation = conversationsMap[row._cvid] || {};
                        row._touid = conversation._uid1 == row._fromuid ? conversation._uid2 : conversation._uid1;

                        if (row._touid) {
                            map[row._mid] = row;
                        }
                    });

                    callback(null, map);
                });
        });
    };

    Exporter.countCategories = function(callback) {
        callback = !_.isFunction(callback) ? noop : callback;
        var prefix = Exporter.config('prefix');
        var query = 'SELECT count(*) FROM ' + prefix + 'node ';

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                callback(null, rows[0]['count(*)']);
            });
    };

    Exporter.getCategories = function(callback) {
        return Exporter.getPaginatedCategories(0, -1, callback);
    };

    Exporter.getPaginatedCategories = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();

        var query = 'SELECT '
            + prefix + 'node.node_id as _cid, '
            + prefix + 'node.title as _name, '
            + prefix + 'node.description as _description, '
            + prefix + 'node.parent_node_id as _parentCid, '
            + prefix + 'node.display_order as _order '
            + 'FROM ' + prefix + 'node '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._name = row._name || 'Untitled Category ';
                    row._description = row._description || 'No decsciption available';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    map[row._cid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.countTopics = function(callback) {
        callback = !_.isFunction(callback) ? noop : callback;
        var prefix = Exporter.config('prefix');
        var query = 'SELECT count(*) '
            + 'FROM ' + prefix + 'thread '
            + 'JOIN ' + prefix + 'post ON ' + prefix + 'thread.first_post_id=' + prefix + 'post.post_id ';

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                callback(null, rows[0]['count(*)']);
            });
    };

    Exporter.getTopics = function(callback) {
        if (Exporter._topicsMap) {
            return callback(null, Exporter._topicsMap);
        }
        return Exporter.getPaginatedTopics(0, -1, function(err, map) {
            Exporter._topicsMap = map;
            callback(err, map);
        });
    };

    Exporter.getPaginatedTopics = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'thread.thread_id as _tid, '
            + prefix + 'thread.user_id as _uid, '
            + prefix + 'thread.node_id as _cid, '
            + prefix + 'thread.title as _title, '
            + prefix + 'thread.username as _guest, '
            + prefix + 'thread.post_date as _timestamp, '
            + prefix + 'thread.view_count as _viewcount, '
            + prefix + 'thread.discussion_open as _open, '
            + prefix + 'post.post_id as _pid, '
            + prefix + 'post.message as _content '
            + 'FROM ' + prefix + 'thread '
            + 'JOIN ' + prefix + 'post ON ' + prefix + 'thread.first_post_id=' + prefix + 'post.post_id '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    row._locked = row._open ? 0 : 1;

                    map[row._tid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.getPosts = function(callback) {
        return Exporter.getPaginatedPosts(0, -1, callback);
    };

    Exporter.getPaginatedPosts = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'post.post_id as _pid, '
            + prefix + 'post.thread_id as _tid, '
            + prefix + 'post.user_id as _uid, '
            + prefix + 'post.username as _guest, '
            + prefix + 'post.message as _content, '
            + prefix + 'post.message_state as _xf_state, '
            + prefix + 'post.post_date as _timestamp '
            + 'FROM ' + prefix + 'post '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        Exporter.getTopics(function(err, topicsMap) {
            Exporter.query(query,
                function(err, rows) {
                    if (err) {
                        Exporter.error(err);
                        return callback(err);
                    }

                    //normalize here
                    var map = {};
                    rows.forEach(function(row) {
                        row._content = row._content || '';
                        row._timestamp = ((row._timestamp || 0) * 1000) || startms;

                        if (row._xf_state === "deleted") {
                            row._deleted = 1;
                        }
                        if (topicsMap[row._tid]._pid != row._pid) {
                            map[row._pid] = row;
                        }
                    });

                    callback(null, map);
                });
        });
    };

    Exporter.teardown = function(callback) {
        Exporter.log('teardown');
        Exporter.connection.end();

        Exporter.log('Done');
        callback();
    };

    Exporter.testrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getUsers(next);
            },
            function(next) {
                Exporter.getMessages(next);
            },
            function(next) {
                Exporter.getCategories(next);
            },
            function(next) {
                Exporter.getTopics(next);
            },
            function(next) {
                Exporter.getPosts(next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.paginatedTestrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getPaginatedUsers(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedMessages(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedCategories(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedTopics(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedPosts(1001, 2000, next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.warn = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.warn.apply(console, args);
    };

    Exporter.log = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.log.apply(console, args);
    };

    Exporter.error = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.error.apply(console, args);
    };

    Exporter.config = function(config, val) {
        if (config != null) {
            if (typeof config === 'object') {
                Exporter._config = config;
            } else if (typeof config === 'string') {
                if (val != null) {
                    Exporter._config = Exporter._config || {};
                    Exporter._config[config] = val;
                }
                return Exporter._config[config];
            }
        }
        return Exporter._config;
    };

    // from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
    Exporter.validateUrl = function(url) {
        var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
        return url && url.length < 2083 && url.match(pattern) ? url : '';
    };

    Exporter.truncateStr = function(str, len) {
        if (typeof str != 'string') return str;
        len = _.isNumber(len) && len > 3 ? len : 20;
        return str.length <= len ? str : str.substr(0, len - 3) + '...';
    };

    Exporter.whichIsFalsy = function(arr) {
        for (var i = 0; i < arr.length; i++) {
            if (!arr[i])
                return i;
        }
        return null;
    };

})(module.exports);
