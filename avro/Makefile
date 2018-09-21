# Makefile assumes that each C file is a full unit test program in itself.
# This Makefile will generate one executable corresponding to each C file in
# this directory.

CC = gcc -g -Wall

DEPDIR = .deps
DF = $(DEPDIR)/$(*F)

SRCS := $(wildcard *.c)
OBJS := $(patsubst %.c, %.o, $(SRCS))
BINS := $(patsubst %.c, exec_%, $(SRCS))
LIBS := -lavro

define MSG
************************************
* avro unit tests built successfully
************************************
endef
export MSG

.PHONY: clean

all: $(BINS)
	@echo "$$MSG"

exec_%: %.o
	$(CC) -o $@ $? $(LIBS)

-include $(SRCS:%.c=$(DEPDIR)/%.P)
%.o : %.c
	$(CC) -o $@ -c -MMD $< $(CFLAGS)
	@mkdir -p $(DEPDIR)
	@cp $*.d $(DF).P; \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
	-e '/^$$/ d' -e 's/$$/ :/' < $*.d >> $(DF).P; \
	rm -f $*.d

clean:
	@rm -f $(OBJS) $(BINS)
