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
	sizes.push_back(std::make_pair((32*1024)>>9, 4));
	sizes.push_back(std::make_pair((16*1024)>>9, 5));
	sizes.push_back(std::make_pair(8192>>9, 50));
	sizes.push_back(std::make_pair(4096>>9, 40));
	sizes.push_back(std::make_pair((1024*1024)>>9, 1));

	string path("/dev/loop0");
	disk   sdb(path, sizes);
	sdb.verify();
	return 0;
}
