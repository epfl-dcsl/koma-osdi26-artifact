CONFIG_MODULE_SIG=n
CONFIG_MODULE_SIG_ALL=n
CONFIG_DEBUG_INF=y
CFLAGS += -g
CC = gcc
obj-m += rakaia.o
rakaia-y = rakaiasock.o strparser.o tcp_send.o
PWD := $(CURDIR)


all:
	$(MAKE) -C /lib/modules/$(shell uname -r)/build CC=$(CC) M=$(PWD) modules

clean:
	$(MAKE) -C /lib/modules/$(shell uname -r)/build CC=$(CC) M=$(PWD) clean
	$(RM) other/cat_noblock *.plist
	@echo "Preserving go.mod"
	@git checkout -- **/go.mod **/go.sum 2>/dev/null || true

indent:
	clang-format -i *.[ch]

# The following targets are useful for debugging Makefiles; they
# print the value of a make variable in one of several contexts.
print-%:
	@echo $* = $($*)

printBuild-%:
	$(MAKE) -C $(KDIR) M=$(shell pwd) $@

printClean-%:
	$(MAKE) -C $(KDIR) M=$(shell pwd) $@



