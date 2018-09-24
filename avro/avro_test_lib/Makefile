CC = gcc -g -Wall -Werror

DEPDIR = .deps
DF = $(DEPDIR)/$(*F)

SRCS := $(wildcard *.c)
OBJS := $(patsubst %.c, %.o, $(SRCS))

LIB_NAME := libavro_test
VERSION_MAJOR := 1
VERSION_MINOR := 0

SO_NAME := $(LIB_NAME).so.$(VERSION_MAJOR)
REAL_NAME := $(LIB_NAME).so.$(VERSION_MAJOR).$(VERSION_MINOR)

define MSG
************************************
* libavro_test built successfully
************************************
endef
export MSG

.PHONY: clean

all: $(REAL_NAME)
	@echo "$$MSG"

$(REAL_NAME): $(OBJS)
	$(CC) --shared -Wl,-soname,$(SO_NAME) -o $@ $?

-include $(SRCS:%.c=$(DEPDIR)/%.P)
%.o : %.c
	$(CC) -o $@ -c -MMD $< $(CFLAGS)
	@mkdir -p $(DEPDIR)
	@cp $*.d $(DF).P; \
	sed -e 's/#.*//' -e 's/^[^:]*: *//' -e 's/ *\\$$//' \
	-e '/^$$/ d' -e 's/$$/ :/' < $*.d >> $(DF).P; \
	rm -f $*.d

clean:
	-rm -f $(OBJS) $(REAL_NAME)
