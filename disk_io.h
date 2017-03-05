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
	std::shared_ptr<char> get_buffer();
	size_t size();
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
	struct event_base *ebp;
	int               ioevfd;
	struct event      *ioevp;
	aio_context_t     context;
private:
	set<IOPtr, IOCompare> ios;

protected:
	void write_done(IOPtr *newiop);

public:
	disk(string path);
	~disk();
	int submit_write(uint64_t nwrites);
//	void print_ios(void);

	void verify();

	uint64_t total_ios(void) {
		return ios.size();
	}
	uint64_t nsectors() {
		return sectors;
	}

	aio_context_t get_aio_context(void) {
		return context;
	}

	int disk_fd(void) {
		return fd;
	}

	int get_io_eventfd(void) {
		return ioevfd;
	}

	struct event *get_io_event(void) {
		return ioevp;
	}

	struct event_base *get_event_base(void) {
		return ebp;
	}
};

#endif