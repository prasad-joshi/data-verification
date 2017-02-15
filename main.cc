#include <iostream>

#include <cassert>

#include "io_generator.h"
#include "disk_io.h"

using std::vector;
using std::pair;
using std::string;

int main(int argc, char *argv[])
{
	string path("/dev/sdb");
	disk   sdb(path);

	vector<pair<uint32_t, uint8_t>> sizes;
	sizes.push_back(std::make_pair(8192>>9, 20));
	sizes.push_back(std::make_pair(4096>>9, 10));

	io_generator g(0, sdb.nsectors(), sizes);

	uint64_t s;
	uint64_t ns;
	for (auto i = 0; i < 1ull; i++) {
		g.next_io(&s, &ns);
		assert(ns >= 1);
		sdb.write(s, ns);
	}

	g.dump_stats();

	return 0;
}
