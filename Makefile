BLDROOT = $(HOME)/build/UConDB
UCDIR = $(BLDROOT)/ucondb
BINDIR = $(BLDROOT)/bin
SRVDIR = $(BLDROOT)/server

TARDIR = /tmp/$(USER)

CLTAR = $(TARDIR)/UConDB_Client_$(VERSION).tar
SRVTAR = $(TARDIR)/UConDB_Server_$(VERSION).tar

all:
	make VERSION=`python ucondb/version.py` all_with_version_defined
	
all_with_version_defined: tars

tars:	clean build $(TARDIR)
	cd $(BLDROOT);  tar cf $(CLTAR) bin ucondb
	cd $(BLDROOT);	tar cf $(SRVTAR) server ucondb
	@echo
	@echo Client tarfile ........... $(CLTAR)
	@echo Server tarfile ........... $(SRVTAR)
	@echo
    
build:  $(SRVROOT) $(CLROOT)
	cd ucondb; make VERSION=$(VERSION) UCDIR=$(UCDIR) BINDIR=$(BINDIR) build
	cd server; make VERSION=$(VERSION) SRVDIR=$(SRVDIR) build

clean:
	rm -rf $(BLDROOT) $(CLTAR) $(SRVTAR)
    
$(SRVROOT):
	mkdir -p $@

$(CLROOT):
	mkdir -p $@
    
$(TARDIR):
	mkdir -p $@
    
    
   
	
