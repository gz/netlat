.PHONY:	all clean udpreceiver udpsender

override CFLAGS += -Wall -g

all:	udpreceiver udpsender

LDFLAGS+=-lpthread

udpreceiver:	udpreceiver.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

udpsender:	udpsender.o common.o
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

common: common.o
	$(CC) $(CFLAGS) $@ $^ $(LDFLAGS)

clean:
	rm -rf *.o udpreceiver udpsender
