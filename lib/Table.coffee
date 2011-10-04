
isEmail = (email) ->
    /^[a-z0-9,!#\$%&'\*\+\/\=\?\^_`\{\|}~\-]+(\.[a-z0-9,!#\$%&'\*\+\/\=\?\^_`\{\|}~\-]+)*@[a-z0-9\-]+(\.[a-z0-9\-]+)*\.([a-z]{2,})$/.test(email)

module.exports = class Table

    redis = null
    _ron = null

    constructor: (ron, options) ->
        _ron = ron
        redis = ron.redis
        options = {name: options} if typeof options is 'string'
        #@db = ron.name
        @name = options.name
        @schema = 
            db: ron.name
            name: options.name
            identifier: null
            index: {}
            unique: {}
            properties: {}
        
    ###
    Define a new property.
    ###
    property: (property, options) ->
        @schema.properties[property] = options
        @indentifier property if property.identifier
    
    ###
    Define a property as an identifier
    ----------------------------------
    An identifier is a property which uniquely define a record.
    Internaly, those type of property are stored in set.
    
    Calling this function without any argument will return the identifier if any.
    ###
    identifier: (property) ->
        # Set the property
        if property?
            @schema.properties[property] = {} unless @schema.properties[property]?
            @schema.properties[property].identifier = true
            @schema.identifier = property
        # Get the property
        else
            @schema.identifier
    
    ###
    Dedine a property as indexable
    ------------------------------
    An indexed property allow records access by its property value. For exemple,
    when using the `list` function, the search can be filtered such as returned
    records match one or multiple values.
    
    Calling this function without any argument will return all the indexed
    properties.
    
    Exemple:
        User.index 'email'
        User.list { filter: { email: 'my@email.com' } }, (err, users) ->
            console.log 'This user has the following accounts:'
            for user in user
                console.log "- #{user.username}"
    ###
    index: (property) ->
        # Set the property
        if property?
            @schema.properties[property] = {} unless @schema.properties[property]?
            @schema.properties[property].index = true
            @schema.index[property] = true
        # Get the property
        else
            @schema.index
    
    ###
    Dedine a property as unique
    ---------------------------
    An unique property is similar to a unique one but,... unique. In addition for
    being filterable, it could also be used as an identifer to access a record.
    
    Calling this function without any argument will return all the unique
    properties.
    
    Exemple:
        User.unique 'username'
        User.get { username: 'me' }, (err, user) ->
            console.log "This is #{user.username}"
    ###
    unique: (property) ->
        # Set the property
        if property?
            @schema.properties[property] = {} unless @schema.properties[property]?
            @schema.properties[property].unique = true
            @schema.unique[property] = true
        # Get the property
        else
            @schema.unique
    
    ###
    Return all records
    ------------------
    Similar to the find method with far less options and a faster implementation.
    ###
    all: (callback) ->
        s = @schema
        redis.smembers "#{s.db}:#{s.name}_#{s.identifier}", (err, recordIds) ->
            multi = redis.multi()
            for recordId in recordIds
                multi.hgetall "#{s.db}:#{s.name}:#{recordId}"
            multi.exec (err, users) ->
                return callback err if err
                callback null, users
    
    ###
    Clear all the records and return the number of removed records
    ###
    clear: (callback) ->
        s = @schema
        cmds = []
        count = 0
        multi = redis.multi()
        # Grab index values for later removal
        indexSort = []
        indexProperties = Object.keys(s.index)
        if indexProperties
            indexSort.push "#{s.db}:#{s.name}_#{s.identifier}"
            for property in indexProperties
                indexSort.push 'get'
                indexSort.push "#{s.db}:#{s.name}:*->#{property}"
                # Delete null index
                cmds.push ['del', "#{s.db}:#{s.name}_#{property}:null"]
            indexSort.push (err, values) ->
                if values.length
                    for i in [0 ... values.length] by indexProperties.length
                        for property, j in indexProperties
                            value = _ron.hash values[i + j]
                            cmds.push ['del', "#{s.db}:#{s.name}_#{property}:#{value}"]
            multi.sort indexSort...
        # Grab record identifiers
        multi.smembers "#{s.db}:#{s.name}_#{s.identifier}", (err, recordIds) ->
            return callback err if err
            # Return count in final callback
            count = recordIds.length
            # delete objects
            for recordId in recordIds
                cmds.push ['del', "#{s.db}:#{s.name}:#{recordId}"]
            # Incremental counter
            cmds.push ['del', "#{s.db}:#{s.name}_incr"]
            # Identifier index
            cmds.push ['del', "#{s.db}:#{s.name}_#{s.identifier}"]
            # Unique indexes
            for property of s.unique
                cmds.push ['del', "#{s.db}:#{s.name}_#{property}"]
            # Index of values
            for property of s.index
                cmds.push ['del', "#{s.db}:#{s.name}_#{property}"]
        multi.exec (err, results) ->
            return callback err if err
            multi = redis.multi cmds
            multi.exec (err, results) ->
                return callback err if err
                callback null, count
    
    ###
    Count the number of records present in the database.
    ###
    count: (callback) ->
        s = @schema
        redis.scard "#{s.db}:#{s.name}_#{s.identifier}", (err, count) ->
            return callback err if err
            callback null, count
    
    ###
    Create a new record.
    ###
    create: (records, callback) ->
        s = @schema
        isArray = Array.isArray records
        records = [records] if ! isArray
        # Sanitize records
        for record in records
            # Validation check
            if not record.email? or typeof record.email isnt 'string' or not isEmail record.email
                return callback new Error 'Email missing or invalid'
            if not record.username or typeof record.username isnt 'string'
                return callback new Error 'Username missing or invalid'
        # Persist
        @exists records, (err, recordIds) ->
            return callback err if err
            for recordId in recordIds
                return callback new Error "User #{recordId} already exists" if recordId?
            multi = redis.multi()
            multi.incr "#{s.db}:#{s.name}_incr" for x in records
            multi.exec (err, recordIds) ->
                return callback err if err
                multi = redis.multi()
                for record, i in records
                    record[s.identifier] = recordId = recordIds[i]
                    multi.sadd "#{s.db}:#{s.name}_#{s.identifier}", recordId
                    # Deal with Unique
                    for property of s.unique
                        multi.hset "#{s.db}:#{s.name}_#{property}", record[property], recordId if record[property]
                    # Deal with Index
                    for property of s.index
                        value = record[property]
                        value = _ron.hash value
                        multi.sadd "#{s.db}:#{s.name}_#{property}:#{value}", recordId
                        #multi.zadd "#{s.db}:#{s.name}_#{property}", 0, record[property]
                    # Store the record
                    r = {}
                    # Filter null values
                    for property, value of record
                        r[property] = value if value?
                    multi.hmset "#{s.db}:#{s.name}:#{recordId}", r
                multi.exec (err, results) ->
                    return callback err if err
                    for result in results
                        return callback new Error 'Corrupted user database ' if result[0] is not "0"
                    callback null, if isArray then records else records[0]
    
    ###
    Check if one or more record exist.
    ----------------------------------
    The existence of a record is based on its id or any property defined as unique.
    The return value respect the same structure as the provided records argument. Ids
    are present if the record exists or null if it doesn't.
    ###
    exists: (records, callback) ->
        s = @schema
        isArray = Array.isArray records
        records = [records] if ! isArray
        multi = redis.multi()
        for record in records
            if typeof record is 'object'
                if record[s.identifier]?
                    recordId = record[s.identifier]
                    multi.hget "#{s.db}:#{s.name}:#{recordId}", s.identifier
                else
                    for property of s.unique
                        if record[property]?
                            multi.hget "#{s.db}:#{s.name}_#{property}", record[property]
            else
                multi.hget "#{s.db}:#{s.name}:#{record}", s.identifier
        multi.exec (err, recordIds) ->
            return callback err if err
            callback null, if isArray then recordIds else recordIds[0]
    
    ###
    Create or extract one or several ids.
    -------------------------------------
    The method doesn't hit the database to check the existance of an id if provided.
    Set the provided object to null if an id couldn't be found.
    
    todo: With no argument, generate an new id
    todo: IF first argument is a number, genererate the number of new id
    If first argument is an object or an array of object, extract the id from those objects
    Options:
    - object: return record objects instead of ids
    - accept_null: prevent error throwing if record is null
    The id will be set to null if the record wasn't discovered in the database
    ###
    id: (records, options, callback) ->
        if arguments.length is 2
            callback = options
            options = {}
        s = @schema
        isArray = Array.isArray records
        records = [records] if not isArray
        cmds = []
        err = null
        for record, i in records
            if typeof record is 'object'
                if not record?
                    if not options.accept_null
                        return callback new Error 'Invalid object, got ' + (JSON.stringify record)
                else if record[s.identifier]?
                    # It's perfect, no need to hit redis
                else if record.username?
                    cmds.push ['hget', "#{s.db}:#{s.name}_username", record.username, ((record) -> (err, recordId) ->
                        record[s.identifier] = recordId
                    )(record)]
                else
                    withUnique = false
                    for property of s.unique
                        if record[property]?
                            withUnique = true
                            cmds.push ['hget', "#{s.db}:#{s.name}_#{property}", record[property], ((record) -> (err, recordId) ->
                                record[s.identifier] = recordId
                            )(record)]
                    # Error if no identifier and no unique value provided
                    return callback new Error 'Invalid object, got ' + (JSON.stringify record) unless withUnique
            else if typeof record is 'number'
                records[i] = {}
                records[i][s.identifier] = record
            else
                return callback new Error 'Invalid id, got ' + (JSON.stringify record)
        # No need to hit redis if no comand are registered
        if cmds.length is 0
            if not options.object
                records = for record in records
                    if record? then record[s.identifier] else record
            return callback null, if isArray then records else records[0]
        multi = redis.multi cmds
        multi.exec (err, results) ->
            if not options.object
                records = for record in records
                    record[s.identifier]
            callback null, if isArray then records else records[0]
    
    ###
    Retrieve one or multiple records
    --------------------------------
    If options is an array, it is considered to be the list of properties to retrieve.
    Options are
    -   properties, array of properties to fetch, all properties if not provided
    -   force, force the retrieval of properties even if already present in the record objects
    callback arguments are
    -   err, error object if the command failed
    -   records, object or array of object if command succeed. Object are null if record was not found
    ###
    get: (records, options, callback) ->
        if arguments.length is 2
            callback = options
            options = {}
        if Array.isArray options
            options = {properties: options}
        s = @schema
        isArray = Array.isArray records
        records = [records] if ! isArray
        @id records, {object: true}, (err, records) ->
            cmds = []
            records.forEach (record, i) ->
                if record[s.identifier] is null
                    records[i] = null
                else if options.properties?
                    options.properties.forEach (property) ->
                        if ! options.force and ! record[property]
                            recordId = record[s.identifier]
                            cmds.push ['hget', "#{s.db}:#{s.name}:#{recordId}", property, (err, value)->
                                record[property] = value
                            ]
                else
                    recordId = record[s.identifier]
                    cmds.push ['hgetall', "#{s.db}:#{s.name}:#{recordId}", (err, values)->
                        for property, value of values
                            record[property] = value
                    ]
            if cmds.length is 0
                return callback null, if isArray then records else records[0]
            multi = redis.multi cmds
            multi.exec (err, values) ->
                return callback err if err
                callback null, if isArray then records else records[0]
    
    list: (options, callback) ->
        if typeof options is 'function'
            callback = options
            options = {}
        s = @schema
        args = []
        multi = redis.multi()
        # Index
        options.where = {} unless options.where?
        where = []
        for property, value of options
            if s.index[property]
                if Array.isArray value
                    for v in value
                        where.push [property, v]
                else
                    where.push [property, value]
        options.where = if Object.keys(options.where).length then options.where else false
        if where.length is 1
            [property, value] = where[0]
            value = _ron.hash value
            args.push "#{s.db}:#{s.name}_#{property}:#{value}"
        else if where.length > 1
            tempkey = "temp:#{(new Date).getTime()}#{Math.random()}"
            keys = []
            keys.push tempkey
            args.push tempkey
            for filter in where
                [property, value] = filter
                value = _ron.hash value
                keys.push "#{s.db}:#{s.name}_#{property}:#{value}"
            operation = options.operation ? 'union'
            multi["s#{operation}store"] keys...
        else
            args.push "#{s.db}:#{s.name}_#{s.identifier}"
        # Sorting by one property
        if options.sort?
            args.push 'by'
            args.push "#{s.db}:#{s.name}:*->" + options.sort
        # Properties to return
        for property of s.properties
            args.push 'get'
            args.push "#{s.db}:#{s.name}:*->" + property
        # Sorting property is a string
        args.push 'alpha'
        # Sorting direction
        args.push options.direction ? 'asc'
        # Callback
        args.push (err, values) ->
            return callback err if err
            return callback null, [] unless values.length
            keys = Object.keys s.properties
            result = for i in [0 ... values.length] by keys.length
                record = {}
                for property, j in keys
                    record[property] = values[i + j]
                record
            callback null, result
        # Run command
        multi.sort args...
        multi.del tempkey if tempkey
        multi.exec()
    
    ###
    Remove one or several records
    ###
    remove: (records, callback) ->
        s = @schema
        isArray = Array.isArray records
        records = [records] if ! isArray
        @get records, [].concat(Object.keys(s.unique), Object.keys(s.index)), (err, records) ->
            return callback err if err
            multi = redis.multi()
            for record in records
                do (record) ->
                    # delete objects
                    recordId = record[s.identifier]
                    multi.del "#{s.db}:#{s.name}:#{recordId}"
                    # delete indexes
                    multi.srem "#{s.db}:#{s.name}_#{s.identifier}", recordId
                    for property of s.unique
                        multi.hdel "#{s.db}:#{s.name}_#{property}", record[property]
                    for property of s.index
                        value = _ron.hash record[property]
                        multi.srem "#{s.db}:#{s.name}_#{property}:#{value}", recordId, (err, count) ->
                            console.warn('Missing indexed property') if count isnt 1
            multi.exec (err, results) ->
                return callback err if err
                callback null, records.length
    
    ###
    Update one or several records
    ###
    update: (records, callback) ->
        s = @schema
        isArray = Array.isArray records
        records = [records] if ! isArray
        # 1. Get values of indexed properties
        # 2. If indexed properties has changed
        #    2.1 Make sure the new property is not assigned to another record
        #    2.2 Erase old index & Create new index
        # 3. Save the record
        @id records, {object: true}, (err, records) ->
            return callback err if err
            # Stop here if a record is invalid
            for record in records
                return callback new Error 'Invalid record' unless record
            # Find records with a possible updated index
            cmdsCheck = []
            cmdsUpdate = []
            multi = redis.multi()
            for record in records
                # Stop here if we couldn't get an id
                recordId = record[s.identifier]
                return callback new Error 'Unsaved record' unless recordId
                r = {}
                # Filter null values
                for property, value of record
                    if value?
                        r[property] = value 
                    else
                        cmdsUpdate.push ['hdel', "#{s.db}:#{s.name}:#{recordId}", property ]
                cmdsUpdate.push ['hmset', "#{s.db}:#{s.name}:#{recordId}", r ]
                # If an index has changed, we need to update it
                do (record) ->
                    recordId = record[s.identifier]
                    changedProperties = []
                    for property in [].concat(Object.keys(s.unique), Object.keys(s.index))
                        changedProperties.push property if typeof record[property] isnt 'undefined'
                    if changedProperties.length
                        multi.hmget "#{s.db}:#{s.name}:#{recordId}", changedProperties, (err, values) ->
                            for property, propertyI in changedProperties
                                if values[propertyI] isnt record[property]
                                    if s.properties[property].unique
                                        # First we check if index for new key exists to avoid duplicates
                                        cmdsCheck.push ['hexists', "#{s.db}:#{s.name}users_#{property}", record[property] ]
                                        # Second, if it exists, erase old key and set new one
                                        cmdsUpdate.push ['hdel', "#{s.db}:#{s.name}_#{property}", values[propertyI] ]
                                        cmdsUpdate.push ['hsetnx', "#{s.db}:#{s.name}_#{property}", record[property], recordId, (err, success) ->
                                            console.warn 'Trying to write on existing unique property' unless success
                                        ]
                                    else if s.properties[property].index
                                        valueOld = _ron.hash values[propertyI]
                                        valueNew = _ron.hash record[property]
                                        cmdsUpdate.push ['srem', "#{s.db}:#{s.name}_#{property}:#{valueOld}", recordId ]
                                        cmdsUpdate.push ['sadd', "#{s.db}:#{s.name}_#{property}:#{valueNew}", recordId ]
            # Get the value of those indexed properties to see if they changed
            multi.exec (err, values) ->
                # Check if unique properties doesn't already exists
                multi = redis.multi cmdsCheck
                multi.exec (err, exists) ->
                    return callback err if err
                    for exist in exists
                        return callback new Error 'Unique value already exists' if exist isnt 0
                    # Update properties
                    multi = redis.multi cmdsUpdate
                    multi.exec (err, results) ->
                        return callback err if err
                        callback null, if isArray then records else records[0]

