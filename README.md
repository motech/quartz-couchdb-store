quartz-couchdb-store
====================

Quartz CouchDb store

An alternative job store implementation for [quartz scheduler](http://quartz-scheduler.org/). Quartz scheduler
supports JDBC store and Ram job store by default. If you are looking for [CouchDB](http://couchdb.apache.org/) quartz backend this might be
useful for you.

Why?

    * No/Less overhead of locking across tables.
    * First version of couchdb jobstore is 5x faster, so it can run more jobs on single node.
    * No schema maintenance - useful while running multiple instances of quartz (cloud solutions)
    * No limit on Scalability - can scale with couch
