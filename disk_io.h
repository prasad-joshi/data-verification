#ifndef __DISK_IO_H__
#define __DISK_IO_H__

#include <cstdint>
#include <string>
#include <memory>
#include <set>
#include <vector>
#include <atomic>
#include <utility>

#include "zipf.h"

using std::string;
using std::set;
using std::vector;
using std::shared_ptr;
using std::pair;

class range {
public:
	uint64_t sector;
	uint32_t nsectors;
	range(uint64_t sect, uint32_t nsect) {
		sector   = sect;
		nsectors = nsect;
	}

	range(): range(0, 0) {}

	uint64_t end_sector(void) const {
		return this->sector + this->nsectors - 1;
	}
	uint64_t start_sector(void) const {
		return this->sector;
	}

	bool operator< (const range &rhs) const {
		uint64_t le = this->end_sector();
		// std::cout << le << "<" << rhs.sector << " " << (le < rhs.sector) << std::endl;
		return le < rhs.sector;
	}
};

class IO {
public:
	range   r;
	string  pattern;
	int16_t pattern_start;
	IO(uint64_t sect, uint32_t nsec, string &pattern, int16_t pattern_start);

public:
	std::shared_ptr<char> get_buffer(size_t *sizep);
};
typedef std::shared_ptr<IO> IOPtr;

struct IOCompare {
	using is_transparent = void;

	bool operator() (const IOPtr &a, const IOPtr &b) const {
		return a->r < b->r;
	}

	bool operator() (const IOPtr &a, const range &b) const {
		return a->r < b;
	}

	bool operator() (const range &a, const IOPtr &b) const {
		return a < b->r;
	}
};

class disk {
private:
	string       path;
	uint64_t     size;
	uint64_t     sectors;
	int          fd;
	uint64_t     total_writes;

private:
	set<IOPtr, IOCompare> ios;

private:
	//void write_done(uint64_t sector, uint16_t nsectors, uint32_t offset);

public:
	disk(string path);
	~disk();
	uint32_t write(uint64_t sector, uint16_t nsectors);
//	void print_ios(void);

	void verify();

	uint64_t total_ios(void) {
		return ios.size();
	}
	uint64_t nsectors() {
		return sectors;
	}

};

#endif