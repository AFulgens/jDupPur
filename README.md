jDupPur
=======

jDupPur is a mini-project for purging duplicate data.

Workflow
--------

1. Generate purgatory file on the given folder
    - I.e. it will generate a file, containing the hashes of all files (recursively) within the given folder
1. Purge the purgatory based on the purgatory file and a primary folder
    - I.e. it will iterate over all files in the primary and delete all files from the purgatory, which are the same

Command line usage
------------------

TODO
