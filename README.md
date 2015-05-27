IC Riak Plugin
==============

[![Build Status](https://travis-ci.org/instaclick/PDI-Plugin-Step-Riak.svg?branch=master)](https://travis-ci.org/instaclick/PDI-Plugin-Step-Riak)

## Compatible with PDI/Kettle 4.x and riak 2.x

Building
--------
The Riak Riak Plugin is built with maven for dependency management.
All you'll need to get started is maven.

    $ git clone git@github.com:instaclick/PDI-Plugin-Step-Riak.git
    $ cd PDI-Plugin-Step-Riak
    $ mvn package


This will produce a pentaho plugin in ``target/ic-riak-plugin-pdi-<version>.tar``
This archive can then be extracted into your Pentaho Data Integration plugin directory.

Download Packages
-----------------
https://github.com/instaclick/pdi-marketplace-packages

PDI Step Configuration
-----------------------

| Property              | Description                                                                   |
| ----------------------|:-----------------------------------------------------------------------------:|
| Mode                  | PUT,GET or DELETE                                                             |
| Connection URI        | Riak Connection URI [host]:[port]?[params]                                    |
| Bucket Name           | Riak Bucket name                                                              |
| Bucket Type           | Riak Bucket type                                                              |
| Resolver              | Conflic resolver Step                                                         |
| Key                   | Field that will be used as riak key                                           |
| Value                 | Field that will be used as riak value                                         |
| VClock                | Field that will be used as riak vclock                                        |


Connection URI Example
----------------------
* riak.dev
* riak.dev:8087
* riak.dev:8087?minConnections=1&maxConnections=2&connectionTimeout=200&idleTimeout=500


![Get Resolver](http://s28.postimg.org/r2970e2kt/riak_get_resolver.png)
