UConDB Client Command Line Interface
------------------------------------

.. code-block:: shell

    $ ucondb 

    ucondb [-s <server URL>] <command> ...

        -s <server URL>                     -- URL of the UConDB server. 
                                               Env. variable UCONDB_SERVER_URL can be used.

    Commands:
        version                                       -- print server version information
        folders or ls                                 -- list folders
        objects or ls <folder>                        -- list objects in folder
        versions or ls [-t] [-j] <folder> <object>    -- list object versions
            -j                      -- JSON output for "versions"
            -t                      -- print Tv as date/time
        get <folder> <object> [get options] -- get object version metadata or data
            -m                      -- get metadata, print to stdout
            -k <key>                -- version key
            -i <id>                 -- version id
            -t <numeric>            -- version validity time, default = now
            -T <tag>                -- tag
            -R <numeric>            -- version which existed as of record time, default = now
            -o <output file>        -- output for the version data BLOB, otherwise - stdout

