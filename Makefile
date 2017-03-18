INC := -I.
LIBS := -levent -laio -lpthread -lfolly
CPPCLAGS := -g -ggdb -O0

all: main

main: disk_io.cc main.cc AsyncIO.cpp
	g++ -std=c++14 $(CPPCLAGS) $(INC) -o $@ $^ $(LIBS)

clean:
	rm -rf main
