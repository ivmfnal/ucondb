UConDB Web API
==============

.. autoclass:: webapi.UConDBClient
   :members:

Code Samples
------------

.. code-block:: python

    from ucondb.webapi import UConDBClient
    
    client = UConDBClient("https://dbdata0vm.fnal.gov/path")

    print("Server version:", client.version())

    print("Folders:")
    for folder_name in client.folders():
        print(folder_name)
        
    for version in client.lookup_versions("configurations","config"):
        vid = version["id"]
        tv = version["tv"]
        print(f"Version {vid}: valid from: {tv}")
        
    