INC := -I.
LIBS := -levent -laio -lpthread -lfolly -lgflags
CPPCLAGS := -g -ggdb -O0

all: main

main: disk_io.cc main.cc AsyncIO.cpp block_trace.cc
	g++ -std=c++14 $(CPPCLAGS) $(INC) -o $@ $^ $(LIBS)

clean:
	rm -rf main
