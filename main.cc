#include <iostream>

#include <cassert>

#include "io_generator.h"
#include "disk_io.h"

using std::vector;
using std::pair;
using std::string;

int main(int argc, char *argv[])
{
	vector<pair<uint32_t, uint8_t>> sizes;
	sizes.push_back(std::make_pair(8192>>9, 20));
	sizes.push_back(std::make_pair(4096>>9, 10));

	string path("/dev/sdb");
	disk   sdb(path, sizes);
	sdb.verify();
	return 0;
}
