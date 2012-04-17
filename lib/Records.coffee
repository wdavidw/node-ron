Schema = require './Schema'

###

Records access and manipulation
===============================

Implement object based storage with indexing support.   

Identifier
----------

Auto generated identifiers are incremented integers. The next identifier is obtained from
a key named as `{s.db}:{s.name}_incr`. All the identifiers are stored as a Redis set in 
a key named as `{s.db}:{s.name}_#{identifier}`.   

Data
----

Records data is stored as a single hash named as `{s.db}:{s.name}:{idenfitier}`. The hash
keys map to the record properties and the hash value map to the values associated with
each properties.   

Regular indexes
---------------

Regular index are stored inside multiple sets, named as
`{s.db}:{s.name}_{property}:{value}`. There is one key for each indexed value and its 
associated value is a set containing all the identifiers of the records whose property
match the indexed value.   

Unique indexes
--------------

Unique indexes are stored inside a single hash key named as 
`{s.db}:{s.name}_{property}`. Inside the hash, keys are the unique values 
associated to the indexed property and values are the record identifiers.   

###
module.exports = class Records extends Schema

    constructor: (ron, schema) ->
        @redis = ron.redis
        super ron, schema
    ###

    `all(callback)`
    ---------------
    Return all records. Similar to the find method with far less options 
    and a faster implementation.   

    ###
    all: (callback) ->
        {redis} = @
        {db, name, identifier} = @data
        redis.smembers "#{db}:#{name}_#{identifier}", (err, recordIds) =>
            multi = redis.multi()
            for recordId in recordIds
                multi.hgetall "#{db}:#{name}:#{recordId}"
            multi.exec (err, records) =>
                return callback err if err
                @unserialize records
                callback null, records
    ###

    `rand(callback)`
    ---------------
    Return one records randomly.  This function
    takes no other argument than the callback called on error or success.

    ###
    rand: (callback) ->
        {redis} = @
        {db, name, identifier} = @data
        redis.srandmember "#{db}:#{name}_#{identifier}", (err, recordId) =>
            multi = redis.multi()
            multi.hgetall "#{db}:#{name}:#{recordId}"
            multi.exec (err, records) =>
                return callback err if err
                @unserialize records
                callback null, records
                
    ###
    `clear(callback)`
    -----------------
    Remove all the records and the references poiting to them. This function
    takes no other argument than the callback called on error or success.   

    `callback`              Received parameters are:   

    *   `err`               Error object if any.   
    *   `count`             Number of removed records on success   
    
    Usage:   
        ron.get('users').clear (err, count) ->
            return console.error "Failed: #{err.message}" if err
            console.log "#{count} records removed"

    ###
    clear: (callback) ->
        {redis, hash} = @
        {db, name, identifier, index, unique} = @data
        cmds = []
        count = 0
        multi = redis.multi()
        # Grab index values for later removal
        indexSort = []
        indexProperties = Object.keys(index)
        if indexProperties.length
            indexSort.push "#{db}:#{name}_#{identifier}"
            for property in indexProperties
                indexSort.push 'get'
                indexSort.push "#{db}:#{name}:*->#{property}"
                # Delete null index
                cmds.push ['del', "#{db}:#{name}_#{property}:null"]
            indexSort.push (err, values) ->
                if values.length
                    for i in [0 ... values.length] by indexProperties.length
                        for property, j in indexProperties
                            value = hash values[i + j]
                            cmds.push ['del', "#{db}:#{name}_#{property}:#{value}"]
            multi.sort indexSort...
        # Grab record identifiers
        multi.smembers "#{db}:#{name}_#{identifier}", (err, recordIds) ->
            return callback err if err
            # Return count in final callback
            count = recordIds.length
            # delete objects
            for recordId in recordIds
                cmds.push ['del', "#{db}:#{name}:#{recordId}"]
            # Incremental counter
            cmds.push ['del', "#{db}:#{name}_incr"]
            # Identifier index
            cmds.push ['del', "#{db}:#{name}_#{identifier}"]
            # Unique indexes
            for property of unique
                cmds.push ['del', "#{db}:#{name}_#{property}"]
            # Index of values
            for property of index
                cmds.push ['del', "#{db}:#{name}_#{property}"]
        multi.exec (err, results) ->
            return callback err if err
            multi = redis.multi cmds
            multi.exec (err, results) ->
                return callback err if err
                callback null, count
    ###

    `count(callback)`
    -----------------
    Count the number of records present in the database.    

    Counting all the records:   

        Users.count, (err, count) ->
            console.log 'count users', count

    `count(property, values, callback)`
    ----------------------------------
    Count the number of one or more values for an indexed property.  

    Counting multiple values:   

        Users.get 'users', properties:
            user_id: identifier: true
            job: index: true
        Users.count 'job' [ 'globtrotter', 'icemaker' ], (err, counts) ->
            console.log 'count globtrotter', counts[0]
            console.log 'count icemaker', counts[1]

    ###
    count: (callback) ->
        {redis} = @
        {db, name, identifier, index} = @data
        if arguments.length is 3
            property = callback
            values = arguments[1]
            callback = arguments[2]
            return callback new Error "Property is not indexed" unless index[property]
            isArray = Array.isArray values
            values = [values] unless isArray
            multi = redis.multi()
            for value, i  in values
                value = @hash value
                multi.scard "#{db}:#{name}_#{property}:#{value}"
            multi.exec (err, counts) ->
                return callback err if err
                callback null, if isArray then counts else counts[0]
        else
            @redis.scard "#{db}:#{name}_#{identifier}", (err, count) ->
                return callback err if err
                callback null, count
    ###

    `create(records, [options], callback)`
    --------------------------------------
    Insert one or multiple record. The records must not already exists 
    in the database or an error will be returned in the callback. Only
    the defined properties are inserted.

    The records passed to the function are returned in the callback enriched their new identifier property.

    `records`               Record object or array of record objects.   

    `options`               Options properties include:   

    *   `identifiers`       Return only the created identifiers instead of the records.   
    *   `validate`          Validate the records.   
    *   `properties`        Array of properties to be returned.   
    *   `milliseconds`      Convert date value to milliseconds timestamps instead of `Date` objects.   
    *   `seconds`           Convert date value to seconds timestamps instead of `Date` objects.   

    `callback`              Called on success or failure. Received parameters are:   

    *   `err`               Error object if any.   
    *   `records`           Records with their newly created identifier.   

    Records are not validated, it is the responsability of the client program calling `create` to either
    call `validate` before calling `create` or to passs the `validate` options.   

    ###
    create: (records, options, callback) ->
        if arguments.length is 2
            callback = options
            options = {}
        {redis, hash} = @
        {db, name, temporal, properties, identifier, index, unique} = @data
        isArray = Array.isArray records
        records = [records] unless isArray
        # Validate records
        if options.validate
            try @validate records, throw: true
            catch e then return callback e, (if isArray then records else records[0])
        # Persist
        @exists records, (err, recordIds) =>
            return callback err if err
            for recordId in recordIds
                return callback new Error "Record #{recordId} already exists" if recordId?
            multi = redis.multi()
            # Get current date once if schema is temporal
            date = new Date Date.now() if temporal?
            # Generate new identifiers
            @id records, (err, records) =>
                multi = redis.multi()
                for record, i in records
                    # Enrich the record with a creation date
                    record[temporal.creation] = date if temporal?.creation? and not record[temporal.creation]?
                    # Enrich the record with a creation date
                    record[temporal.modification] = date if temporal?.modification? and not record[temporal.modification]?
                    # Register new identifier
                    multi.sadd "#{db}:#{name}_#{identifier}", record[identifier]
                    # Deal with Unique
                    for property of unique
                        multi.hset "#{db}:#{name}_#{property}", record[property], record[identifier] if record[property]
                    # Deal with Index
                    for property of index
                        value = record[property]
                        value = hash value
                        multi.sadd "#{db}:#{name}_#{property}:#{value}", record[identifier]
                        #multi.zadd "#{s.db}:#{s.name}_#{property}", 0, record[property]
                    # Store the record
                    r = {}
                    for property, value of record
                        # Insert only defined properties
                        continue unless properties[property]
                        # Filter null values
                        r[property] = value if value?
                    @serialize r
                    multi.hmset "#{db}:#{name}:#{record[identifier]}", r
                multi.exec (err, results) =>
                    return callback err if err
                    for result in results
                        return callback new Error 'Corrupted user database ' if result[0] is not "0"
                    @unserialize records, options
                    callback null, if isArray then records else records[0]
    ###
    
    `exists(records, callback)`
    ---------------------------
    Check if one or more record exist. The existence of a record is based on its 
    id or any property defined as unique. The provided callback is called with 
    an error or the records identifiers. The identifiers respect the same 
    structure as the provided records argument. If a record does not exists, 
    its associated return value is null.   

    `records`               Record object or array of record objects.   

    `callback`              Called on success or failure. Received parameters are:   

    *   `err`               Error object if any.   
    *   `identifier`        Record identifiers or null values.   

    ###
    exists: (records, callback) ->
        {redis} = @
        {db, name, identifier, unique} = @data
        isArray = Array.isArray records
        records = [records] unless isArray
        multi = redis.multi()
        for record in records
            if typeof record is 'object'
                if record[identifier]?
                    recordId = record[identifier]
                    multi.hget "#{db}:#{name}:#{recordId}", identifier
                else
                    for property of unique
                        if record[property]?
                            multi.hget "#{db}:#{name}_#{property}", record[property]
            else
                multi.hget "#{db}:#{name}:#{record}", identifier
        multi.exec (err, recordIds) =>
            return callback err if err
            @unserialize recordIds
            callback null, if isArray then recordIds else recordIds[0]
    ###

    `get(records, [options], callback)`
    -----------------------------------
    Retrieve one or multiple records. If options is an array, it is considered 
    to be the list of properties to retrieve. By default, unless the `force` 
    option is defined, only the properties not yet defined in the provided 
    records are fetch from Redis.   

    `options`               All options are optional. Options properties include:   
    
    *   `properties`        Array of properties to fetch, all properties unless defined.   
    *   `force`             Force the retrieval of properties even if already present in the record objects.   
    *   `accept_null`       Skip objects if they are provided as null.   
    *   `object`            If `true`, return an object where keys are the identifier and value are the fetched records

    `callback`              Called on success or failure. Received parameters are:   

    *   `err`               Error object if the command failed.   
    *   `records`           Object or array of object if command succeed. Objects are null if records are not found.   
    
    ###
    get: (records, options, callback) ->
        if arguments.length is 2
            callback = options
            options = {}
        if Array.isArray options
            options = {properties: options}
        {redis} = @
        {db, name, identifier} = @data
        isArray = Array.isArray records
        records = [records] unless isArray
        # Quick exit for accept_null
        if options.accept_null? and not records.some((record) -> record isnt null)
            return callback null, if isArray then records else records[0]
        # Retrieve records identifiers
        @identify records, {object: true, accept_null: options.accept_null?}, (err, records) =>
            return callback err if err
            cmds = []
            records.forEach (record, i) ->
                # An error would have been thrown by id if record was null and accept_null wasn't provided
                return unless record?
                if record[identifier] is null
                    records[i] = null
                else if options.properties?.length
                    options.properties.forEach (property) ->
                        unless options.force or record[property]
                            recordId = record[identifier]
                            cmds.push ['hget', "#{db}:#{name}:#{recordId}", property, (err, value)->
                                record[property] = value
                            ]
                else
                    recordId = record[identifier]
                    cmds.push ['hgetall', "#{db}:#{name}:#{recordId}", (err, values)->
                        for property, value of values
                            record[property] = value
                    ]
            if cmds.length is 0
                return callback null, if isArray then records else records[0]
            multi = redis.multi cmds
            multi.exec (err, values) =>
                return callback err if err
                @unserialize records
                if options.object
                    recordsByIds = {}
                    for record in records
                        recordsByIds[record[identifier]] = record
                    callback null, recordsByIds
                else
                    callback null, if isArray then records else records[0]
    ###
    `id(records, callback)`
    -----------------------
    Generate new identifiers. The first arguments `records` may be the number
    of ids to generate, a record or an array of records.

    ###
    id: (records, callback) ->
        {redis} = @
        {db, name, identifier, unique} = @data
        if typeof records is 'number'
            incr = records
            isArray = true
            records = for i in [0...records] then null
        else
            isArray = Array.isArray records
            records = [records] unless isArray
            incr = 0
            for record in records then incr++ unless record[identifier]
        redis.incrby "#{db}:#{name}_incr", incr, (err, recordId) =>
            recordId = recordId - incr
            return callback err if err
            for record, i in records
                records[i] = record = {} unless record
                recordId++ unless record[identifier]
                # Enrich the record with its identifier
                record[identifier] = recordId unless record[identifier]
            callback null, if isArray then records else records[0]
    ###

    `identify(records, [options], callback)`
    ----------------------------------------
    Extract record identifiers or set the identifier to null if its associated record could not be found.   

    The method doesn't hit the database to validate record values and if an id is 
    provided, it wont check its existence. When a record has no identifier but a unique value, then its
    identifier will be fetched from Redis.   

    `records`               Record object or array of record objects.   

    `options`               Options properties include:   

    *   `accept_null`       Skip objects if they are provided as null.   
    *   `object`            Return an object in the callback even if it recieve an id instead of a record.   

    Use reverse index lookup to extract user ids:   

        Users.get 'users', properties:
            user_id: identifier: true
            username: unique: true
        Users.id [
            {username: 'username_1'}
            {username: 'username_2'}
        ], (err, ids) ->
            should.not.exist err
            console.log ids

    Use the `object` option to return records instead of ids:   

        Users.get 'users', properties:
            user_id: identifier: true
            username: unique: true
        Users.id [
            1, {user_id: 2} ,{username: 'username_3'}
        ], object: true, (err, users) ->
            should.not.exist err
            ids = for user in users then user.user_id
            console.log ids

    ###
    identify: (records, options, callback) ->
        if arguments.length is 2
            callback = options
            options = {}
        {redis} = @
        {db, name, identifier, unique} = @data
        isArray = Array.isArray records
        records = [records] unless isArray
        cmds = []
        err = null
        for record, i in records
            if typeof record is 'object'
                unless record?
                    # Check if we allow records to be null
                    unless options.accept_null 
                        return callback new Error 'Null record'
                else if record[identifier]?
                    # It's perfect, no need to hit redis
                else
                    withUnique = false
                    for property of unique
                        if record[property]?
                            withUnique = true
                            cmds.push ['hget', "#{db}:#{name}_#{property}", record[property], ((record) -> (err, recordId) ->
                                record[identifier] = recordId
                            )(record)]
                    # Error if no identifier and no unique value provided
                    return callback new Error 'Invalid record, got ' + (JSON.stringify record) unless withUnique
            else if typeof record is 'number' or typeof record is 'string'
                records[i] = {}
                records[i][identifier] = record
            else
                return callback new Error 'Invalid id, got ' + (JSON.stringify record)
        finalize = ->
            unless options.object
                records = for record in records
                    if record? then record[identifier] else record
            callback null, if isArray then records else records[0]
        # No need to hit redis if there is no command
        return finalize() if cmds.length is 0
        # Run the commands
        multi = redis.multi cmds
        multi.exec (err, results) =>
            return callback err if err
            @unserialize records
            finalize()
    ###

    `list([options], callback)`
    ---------------------------
    List records with support for filtering and sorting.   

    `options`               Options properties include:   

    *   `direction`         One of `asc` or `desc`, default to `asc`.   
    *   `identifiers`       Return an array of identifiers instead of the record objects.  
    *   `milliseconds`      Convert date value to milliseconds timestamps instead of `Date` objects.   
    *   `properties`        Array of properties to be returned.   
    *   `operation`         Redis operation in case of multiple `where` properties, default to `union`.   
    *   `seconds`           Convert date value to seconds timestamps instead of `Date` objects.   
    *   `sort`              Name of the property by which records should be ordered.   
    *   `where`             Hash of property/value used to filter the query.   

    `callback`              Called on success or failure. Received parameters are:   

    *   `err`               Error object if any.   
    *   `records`           Records fetched from Redis.   

    Using the `union` operation:   

        Users.list
            where: group: ['admin', 'redis']
            operation: 'union'
            direction: 'desc'
        , (err, users) ->
            console.log users
    
    An alternative syntax is to bypass the `where` option, the exemple above
    could be rewritten as:   

        Users.list
            group: ['admin', 'redis']
            operation: 'union'
            direction: 'desc'
        , (err, users) ->
            console.log users

    ###
    list: (options, callback) ->
        if typeof options is 'function'
            callback = options
            options = {}
        {redis, hash} = @
        {db, name, identifier, index} = @data
        options.properties = options.properties or Object.keys @data.properties
        options.properties = [identifier] if options.identifiers
        args = []
        multi = @redis.multi()
        # Index
        options.where = {} unless options.where?
        where = []
        for property, value of options
            if index[property]
                if Array.isArray value
                    for v in value
                        where.push [property, v]
                else
                    where.push [property, value]
        options.where = if Object.keys(options.where).length then options.where else false
        if where.length is 1
            [property, value] = where[0]
            value = hash value
            args.push "#{db}:#{name}_#{property}:#{value}"
        else if where.length > 1
            tempkey = "temp:#{(new Date).getTime()}#{Math.random()}"
            keys = []
            keys.push tempkey
            args.push tempkey
            for filter in where
                [property, value] = filter
                value = hash value
                keys.push "#{db}:#{name}_#{property}:#{value}"
            operation = options.operation ? 'union'
            multi["s#{operation}store"] keys...
        else
            args.push "#{db}:#{name}_#{identifier}"
        # Sorting by one property
        if options.sort?
            args.push 'by'
            args.push "#{db}:#{name}:*->" + options.sort
        # Properties to return
        for property in options.properties
            args.push 'get'
            args.push "#{db}:#{name}:*->" + property
        # Sorting property is a string
        args.push 'alpha'
        # Sorting direction
        args.push options.direction ? 'asc'
        # Callback
        args.push (err, values) =>
            return callback err if err
            return callback null, [] unless values.length
            result = for i in [0 ... values.length] by options.properties.length
                record = {}
                for property, j in options.properties
                    record[property] = values[i + j]
                @unserialize record, options
            callback null, result
        # Run command
        multi.sort args...
        multi.del tempkey if tempkey
        multi.exec()
    ###
    
    `remove(records, callback)`
    ---------------------------
    Remove one or several records from the database. The function will also 
    handle all the indexes referencing those records.   
    
    ###
    remove: (records, callback) ->
        {redis, hash} = @
        {db, name, identifier, index, unique} = @data
        isArray = Array.isArray records
        records = [records] unless isArray
        @get records, [].concat(Object.keys(unique), Object.keys(index)), (err, records) ->
            return callback err if err
            multi = redis.multi()
            for record in records
                do (record) ->
                    # delete objects
                    recordId = record[identifier]
                    multi.del "#{db}:#{name}:#{recordId}"
                    # delete indexes
                    multi.srem "#{db}:#{name}_#{identifier}", recordId
                    for property of unique
                        multi.hdel "#{db}:#{name}_#{property}", record[property]
                    for property of index
                        value = hash record[property]
                        multi.srem "#{db}:#{name}_#{property}:#{value}", recordId, (err, count) ->
                            console.warn('Missing indexed property') if count isnt 1
            multi.exec (err, results) ->
                return callback err if err
                callback null, records.length
    ###
    
    `update(records, [options], callback)` 
    --------------------------------------
    Update one or several records. The records must exists in the database or 
    an error will be returned in the callback. The existence of a record may 
    be discovered through its identifier or the presence of a unique property.   
    
    `records`               Record object or array of record objects.   
    
    `options`               Options properties include:   
    
    *   `validate`          Validate the records.   
    
    `callback`              Called on success or failure. Received parameters are:   
    
    *   `err`               Error object if any.   
    *   `records`           Records with their newly created identifier.   
    
    Records are not validated, it is the responsability of the client program to either
    call `validate` before calling `update` or to passs the `validate` options.   
    
    Updating a single record:   

        Users.update
            username: 'my_username'
            age: 28
        , (err, user) -> console.log user
    
    ###
    update: (records, options, callback) ->
        if arguments.length is 2
            callback = options
            options = {}
        {redis, hash} = @
        {db, name, temporal, properties, identifier, unique, index} = @data
        isArray = Array.isArray records
        records = [records] unless isArray
        # Validate records
        if options.validate
            try @validate records, {throw: true, skip_required: true}
            catch e then return callback e, (if isArray then records else records[0])
        # 1. Get values of indexed properties
        # 2. If indexed properties has changed
        #    2.1 Make sure the new property is not assigned to another record
        #    2.2 Erase old index & Create new index
        # 3. Save the record
        @identify records, {object: true}, (err, records) =>
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
                recordId = record[identifier]
                return callback new Error 'Unsaved record' unless recordId
                # Enrich the record with a modification date
                record[temporal.modification] = new Date Date.now() if temporal?.modification? and not record[temporal.modification]?
                r = {}
                # Filter null values
                for property, value of record
                    if value?
                        r[property] = value 
                    else
                        cmdsUpdate.push ['hdel', "#{db}:#{name}:#{recordId}", property ]
                @serialize r
                cmdsUpdate.push ['hmset', "#{db}:#{name}:#{recordId}", r ]
                # If an index has changed, we need to update it
                do (record) ->
                    recordId = record[identifier]
                    changedProperties = []
                    # Find the indexed properties that may have changed
                    for property in [].concat(Object.keys(unique), Object.keys(index))
                        changedProperties.push property if typeof record[property] isnt 'undefined'
                    if changedProperties.length
                        # Get the persisted value for those indexed properties
                        multi.hmget "#{db}:#{name}:#{recordId}", changedProperties, (err, values) ->
                            for property, propertyI in changedProperties
                                if values[propertyI] isnt record[property]
                                    if properties[property].unique
                                        # First we check if index for new key exists to avoid duplicates
                                        cmdsCheck.push ['hexists', "#{db}:#{name}_#{property}", record[property] ]
                                        # Second, if it exists, erase old key and set new one
                                        cmdsUpdate.push ['hdel', "#{db}:#{name}_#{property}", values[propertyI] ]
                                        cmdsUpdate.push ['hsetnx', "#{db}:#{name}_#{property}", record[property], recordId, (err, success) ->
                                            console.warn 'Trying to write on existing unique property' unless success
                                        ]
                                    else if properties[property].index
                                        valueOld = hash values[propertyI]
                                        valueNew = hash record[property]
                                        cmdsUpdate.push ['srem', "#{db}:#{name}_#{property}:#{valueOld}", recordId ]
                                        cmdsUpdate.push ['sadd', "#{db}:#{name}_#{property}:#{valueNew}", recordId ]
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

