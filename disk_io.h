#ifndef __DISK_IO_H__
#define __DISK_IO_H__

#include <cstdint>
#include <string>
#include <memory>
#include <set>
#include <vector>
#include <atomic>
#include <utility>
#include <mutex>

#include <libaio.h>
#include <event.h>

#include <folly/init/Init.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/AsyncTimeout.h>

#include "io_generator.h"
#include "zipf.h"

#define MIN_TO_SEC(min)   ((min) * 60)
#define SEC_TO_MILL(sec)  ((sec) * 1000)
#define MIN_TO_MILLI(min) (SEC_TO_MILL(MIN_TO_SEC((min))))

using std::string;
using std::set;
using std::vector;
using std::shared_ptr;
using std::pair;
using std::unique_ptr;
using namespace folly;

typedef shared_ptr<char> ManagedBuffer;

enum IOMode {
	WRITE,
	VERIFY,
};

enum IOType {
	WRITE,
	READ,
};

class range {
public:
	uint64_t sector;
	uint32_t nsectors;
	range(uint64_t sect, uint32_t nsect) {
		sector   = sect;
		nsectors = nsect;
	}

	range(const range &r) {
		this->sector   = r.sector;
		this->nsectors = r.nsectors;
	}

	void operator = (const range &r) {
		this->sector   = r.sector;
		this->nsectors = r.nsectors;
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
	ManagedBuffer buffer;
	IOType        type;

public:
	IO(uint64_t sect, uint32_t nsec, string &pattern, int16_t pattern_start,
			IOType type);
	ManagedBuffer get_io_buffer();
	size_t        size();
	uint64_t      offset();
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
	int32_t      iodepth;
	shared_ptr<io_generator> iogen;

private:
	folly::EventBase base;
	int              ioevfd;
	io_context_t     context;

	std::mutex            lock;
	set<IOPtr, IOCompare> ios;

protected:
	void pattern_create(uint64_t sector, uint16_t nsectors, string &pattern);
	void write_done(uint64_t s, uint16_t ns, string pattern, int16_t pattern_start);

public:
	class TimeoutWrapper : public AsyncTimeout {
	private:
		TimeoutManager *mp_;
		disk           *dp_;
	public:
		TimeoutWrapper(disk *dp, TimeoutManager *mp) :
				dp_(dp), mp_(mp), AsyncTimeout(mp) {
		}

		void timeoutExpired() noexcept {
			dp_->switch_io_mode();
		}
	};

	disk(string path, vector<pair<uint32_t, uint8_t>> sizes);
	~disk();
	int submit_writes(uint64_t nwrites);
	void write_done(vector<IOPtr> newiosp);
	void set_io_mode(IOMode mode);
	void switch_io_mode();
//	void print_ios(void);

	int verify();

	uint64_t total_ios(void) {
		return ios.size();
	}
	uint64_t nsectors() {
		return sectors;
	}

	io_context_t get_aio_context(void) {
		return context;
	}

	int disk_fd(void) {
		return fd;
	}

	int get_io_eventfd(void) {
		return ioevfd;
	}

private:
	IOMode mode_;
	unique_ptr<TimeoutWrapper> timeout;
};

#endif
