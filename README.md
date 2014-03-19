IC Riak Plugin
==============

Building
--------
The Riak Riak Plugin is built with maven for dependency management.
All you'll need to get started is maven.

    $ git clone git@github.com:instaclick/PDI-Plugin-Step-Riak.git
    $ cd PDI-Plugin-Step-Riak
    $ mvn package


This will produce a pentaho plugin in ``target/ic-riak-plugin-pdi-<version>.tar``
This archive can then be extracted into your Pentaho Data Integration plugin directory.


PDI Step Configuration
-----------------------

| Property              | Description                                                                   |
| ----------------------|:-----------------------------------------------------------------------------:|
| Mode                  | If the step will PUT,GET or DELETE rows                                       |
| Host                  | Riak Host                                                                     |
| Port                  | Riak Port                                                                     |
| Bucket                | Riak Bucket name                                                              |
| Key                   | Field that will be used as riak key                                           |
| Value                 | Field that will be used as riak value                                         |
