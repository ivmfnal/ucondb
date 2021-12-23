API_FILES = UConDB.py signature.py webapi.py version.py __init__.py
BACKEND_FILES = UCon_backend.py UCon_blob_server.py UCon_couchbase.py UCon_kbs.py UCon_psql.py __init__.py
TOOLS_FILES = dbdig.py timelib.py py3.py __init__.py

build: $(UCDIR)
	mkdir -p $(UCDIR)/backends
	mkdir -p $(UCDIR)/tools
	cd tools; cp $(TOOLS_FILES) $(UCDIR)/tools
	cd backends; cp $(BACKEND_FILES) $(UCDIR)/backends
	cp $(API_FILES) $(UCDIR)
	cp -R ui $(UCDIR)
	
$(UCDIR):
	mkdir -p $@

	
    
