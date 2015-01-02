OBJMASH (ОБЙМАШ)
================

Yet another Python implementation of the ABBOE system. The distribution is
geared towards a more barebones application. Server implementation is
implemented in a functional way and is single-threaded, built around select
syscall and non-blocking sockets.


Requirements
------------

Python 3.x for OBJMASH proper and services. The exact required version is
unknown.

Python 2.7 and Robot Framework installed to run the test suite.


How to run
----------

- `./OBJMASH`
- `./client_registry_service`


How to test
-----------

Make sure you have Robot Framework installed (I use virtualenv and pip to get
a local self-contained installation).

- `pybot functional-tests`
