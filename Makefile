export CC=gcc
export CPP=g++
export AR=ar
export CFLAGS=-g -O3
export LDFLAGS=-lulfius -ljansson -lorcania -lpthread 

.PHONY: all
all: query_workload_driver

query_workload_driver: query_workload_driver.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)


.PHONY: clean
clean:
	rm -rf query_workload_driver
