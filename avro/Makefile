# Makefile assumes that each C file is a full unit test program in itself.
# This Makefile will generate one executable corresponding to each C file in
# this directory.

CC = gcc -g -Wall -Werror

DEPDIR = .deps
DF = $(DEPDIR)/$(*F)

SRCS := $(wildcard *.c)
OBJS := $(patsubst %.c, %.o, $(SRCS))
BINS := $(patsubst %.c, exec_%, $(SRCS))
LIBS := -L./avro_test_lib -lavro -lavro_test
INCLUDE := -I./avro_test_lib

define MSG
************************************
* avro unit tests built successfully
************************************
endef
export MSG

.PHONY: all clean avro_test_lib

all: avro_test_lib $(BINS)
	@echo "$$MSG"

avro_test_lib:
	make -C ./avro_test_lib all

exec_%: %.o
	$(CC) -o $@ $? $(LIBS)

-include $(SRCS:%.c=$(DEPDIR)/%.P)
%.o : %.c
	$(CC) -o $@ -c -MMD $< $(INCLUDE) 
	@mkdir -p $(DEPDIR)
	@cp $*.d $(DF).P; \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
	-e '/^$$/ d' -e 's/$$/ :/' < $*.d >> $(DF).P; \
	rm -f $*.d

clean:
	make -C ./avro_test_lib/ clean
	rm -f $(OBJS) $(BINS)
