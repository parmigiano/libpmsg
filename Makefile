CC = gcc
CFLAGS = -Wall -Wextra -std=gnu11 -Iinclude -Isrc
OBJDIR = .out
BINDIR = .build
PREFIX ?= /usr/local
DESTDIR ?=
RELEASE_DIR = libpmsg-dev

LIN_LDFLAGS = -pthread

SRCS = src/pmsg_frame.c src/pmsg_stream.c src/pmsg_client.c src/pmsg_server.c src/pmsg_runtime.c
LIN_OBJS = $(patsubst %.c,$(OBJDIR)/%.o,$(SRCS))

$(OBJDIR)/%.o: %.c
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -fPIC -c $< -o $@

libpmsg.so: $(LIN_OBJS)
	$(CC) -shared -fPIC -o libpmsg.so $(LIN_OBJS) $(LIN_LDFLAGS)

lin-lib: libpmsg.so
	rm -rf $(RELEASE_DIR)
	mkdir -p $(RELEASE_DIR)
	cp -r include $(RELEASE_DIR)/
	cp libpmsg.so $(RELEASE_DIR)/
	cp libpmsg.pc $(RELEASE_DIR)/
	cp Makefile README.md README_RU.md $(RELEASE_DIR)/
	tar -czf $(RELEASE_DIR).tar.gz $(RELEASE_DIR)
	rm -rf $(RELEASE_DIR)

lib-install: libpmsg.so
	mkdir -p $(DESTDIR)$(PREFIX)/include/libpmsg
	mkdir -p $(DESTDIR)$(PREFIX)/lib/pkgconfig
	cp include/*.h $(DESTDIR)$(PREFIX)/include/libpmsg
	cp libpmsg.so $(DESTDIR)$(PREFIX)/lib
	cp libpmsg.pc $(DESTDIR)$(PREFIX)/lib/pkgconfig/libpmsg.pc

test:
	@mkdir -p $(BINDIR)
	$(CC) $(CFLAGS) -g tests/test_frame.c $(SRCS) -o $(BINDIR)/test_frame $(LIN_LDFLAGS)
	$(CC) $(CFLAGS) -g tests/test_server.c $(SRCS) -o $(BINDIR)/test_server $(LIN_LDFLAGS)
	$(BINDIR)/test_frame
	$(BINDIR)/test_server

clean:
	rm -rf $(OBJDIR) $(BINDIR) libpmsg.so $(RELEASE_DIR) $(RELEASE_DIR).tar.gz
