#/bin/make
###############################################################################
#                       Make file for OCI and OCCI demos
###############################################################################
# Usage :
#
# For compiling & linking the cdemo81.c file
#   Edit cdemo81.c and update the credentials and connection string.
#   Then run:
#   make -f demo.mk buildoci EXE=cdemo81 OBJS=cdemo81.o
#
# For compiling & linking the occidml.cpp file
#   Edit occidml.cpp and update the credentials and connection string.
#   Then run:
#   make -f demo.mk buildocci EXE=occidml OBJS=occidml.o
#
# For compiling & linking the occiobj.cpp file:
#   Add the default jdk<ver>/bin to your PATH
#   Add Instant Client directory to the system library search path. (Note
#       this is already done with Instant Client RPMs on Linux).
#       For example, on Linux, with Instant Client zip files, add the Instant
#       Client directory to LD_LIBRARY_PATH.
#   Add ottclasses.zip and ojdbc<ver>.jar to your CLASSPATH
#   They are in: .../instantclient_<ver>/sdk/ottclasses.zip and
#       .../instantclient_<ver>/ojdbc<ver>.jar.
#    Change the credential variables OCCIOTTUSR and OCCIOTTPWD in demo.mk.
#    Then run:
#    make -f demo.mk occiobj
#
# In general, for any OCCI program, run:
#    make -f demo.mk buildocci EXE=<exename> OBJS="<list of dependent objs>"
#
# For compiling all demos, make the above changes and then run:
#    make -f demo.mk
#
# NOTE: Please change cc and CC to point to the appropiate location on your
#       machine.
#
###############################################################################

.SUFFIXES: .o .c .cpp

CC=/opt/SunProd/SUNWspro6.1/bin/CC
cc=/opt/SunProd/SUNWspro6.1/bin/cc

ICINCHOME=../
ICLIBHOME=../../
ICLIBPATH=-L$(ICLIBHOME)
CCINCLUDES=-I$(ICINCHOME)include

THREADLIBS=-lthread
CCLIB=$(ICLIBPATH) -locci -lclntsh $(THREADLIBS)

CCFLAGS=$(CCINCLUDES) -D_REENTRANT -g -xs 
LDFLAGS=
SO_EXT=.so

REMOVE=rm -rf
MAKE=make
MAKEFILE=demo.mk
CLNCACHE=cleancache
CACHEDIR=SunWS_cache

CDEMOEXE=cdemo81
CDEMOOBJS=cdemo81.o
OCCIDEMO=occidml
OCCIOBJDEMO=occiobj
OTT=../ott
OCCIOTTUSR=scott
OCCIOTTPWD=tiger

.cpp.o:
	$(CC) -c $(CCFLAGS) $<

.c.o:
	$(cc) -c $(CCFLAGS) $<

all: clean buildoci $(OCCIDEMO) $(OCCIOBJDEMO)

buildoci: $(CLNCACHE) $(LIBCLNT) $(CDEMOOBJS)
	$(CC) -o $(CDEMOEXE) $(LDFLAGS) $(CDEMOOBJS) $(CCLIB)

buildocci: $(CLNCACHE) $(LIBCLNT) $(OBJS)
	$(CC) -o $(EXE) $(LDFLAGS) $(OBJS) $(CCLIB)

$(OCCIDEMO):
	$(MAKE) -f $(MAKEFILE) buildocci OBJS=$@.o EXE=$@

$(OCCIOBJDEMO):
	$(OTT) userid=$(OCCIOTTUSR)/$(OCCIOTTPWD) \
                intype=$@.typ \
                outtype=$@out.type \
                code=cpp \
                hfile=$@.h \
                cppfile=$@o.cpp \
                attraccess=private \
                unicode=none
	$(MAKE) -f $(MAKEFILE) buildocci OBJS="$@.o $@m.o $@o.o" EXE=$@

cleancache:
	$(REMOVE) $(CACHEDIR)

clean: $(CLNCACHE)
	$(REMOVE) cdemo81 cdemo81.o occidml occidml.o occiobj occiobj.o occiobjo* occiobjm* occiobj.h occiobjout.type



#
# This port-specific file is currently empty on Solaris. Product
# lines may use this file to override compiler definitions and
# flags used in occi.mk.
#

# Linux compiler definitions
CC=/usr/bin/g++
cc=/usr/bin/gcc

ifeq ($(BUILD32),T)
CCFLAGS=$(CCINCLUDES) -DLINUX -D_GNU_SOURCE -D_REENTRANT -g -m32
LDFLAGS=-g -m32
else
CCFLAGS=$(CCINCLUDES) -DLINUX -D_GNU_SOURCE -D_REENTRANT -g
LDFLAGS=-g

#Use libocci_gcc53 library for linux 64 bit, if gcc version 5 and above is used.
GCCVERSION=$(shell expr `$(CC) -dumpversion | cut -f1 -d.`)
ifeq ($(shell test $(GCCVERSION) -gt 4; echo $$?), 0)
CCLIB=$(ICLIBPATH) -locci_gcc53 -lclntsh $(THREADLIBS)
CCFLAGS += -Wno-narrowing
endif
endif
CLNCACHE=

THREADLIBS=-lpthread

