INC := -I.
LIBS := -levent -laio -lpthread
CPPCLAGS := -g -ggdb -O0

all: main

main: disk_io.cc main.cc
	g++ -std=c++14 $(CPPCLAGS) $(INC) -o $@ $^ $(LIBS)

clean:
	rm -rf main
