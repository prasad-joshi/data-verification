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
#include "AsyncIO.h"

#define MIN_TO_SEC(min)   ((min) * 60)
#define SEC_TO_MILLI(sec) ((sec) * 1000)
#define MIN_TO_MILLI(min) (SEC_TO_MILLI(MIN_TO_SEC((min))))
#define MIN(n1, n2)       ((n1) <= (n2) ? (n1) : (n2))

using std::string;
using std::set;
using std::vector;
using std::shared_ptr;
using std::pair;
using std::unique_ptr;
using namespace folly;

using TimeOutCB = std::function<void(void *cbdata)>;

enum class IOMode {
	WRITE,
	VERIFY,
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

public:
	IO(uint64_t sect, uint32_t nsec, const string &pattern, int16_t pattern_start);
	size_t   size();
	uint64_t offset();
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
	string       path_;
	uint64_t     size;
	uint64_t     sectors_;
	int          fd;
	uint16_t     iodepth_;
	uint16_t     percent_;
	unique_ptr<io_generator> iogen;

private:
	folly::EventBase      base;
	AsyncIO               asyncio;
	std::mutex            lock;
	set<IOPtr, IOCompare> ios;
	vector<pair<range, bool>> writeIOsSubmitted;

protected:
	void setIOMode(IOMode mode);
	int  writesSubmit(uint64_t nreads);
	int  readsSubmit(uint64_t nreads);
	void setRuntimeTimer();

	ManagedBuffer getIOBuffer(size_t size);
	ManagedBuffer prepareIOBuffer(size_t size, const string &pattern);
	bool patternCompare(const char *const bufp, size_t size, const string &pattern, int16_t start);
	bool readDataVerify(const char *const data, uint64_t sector, uint16_t nsectors);
	void patternCreate(uint64_t sector, uint16_t nsectors, string &pattern);
	void writeDone(uint64_t sector, uint16_t nsectors, const string &pattern, const int16_t pattern_start);

	void addWriteIORange(uint64_t sector, uint16_t nsectors);
	pair<range, bool> removeWriteIORange(uint64_t sector, uint16_t nsectors);
public:
	class TimeoutWrapper : public AsyncTimeout {
	private:
		void      *cbdatap_;
		TimeOutCB cb_;
	public:
		TimeoutWrapper(TimeoutManager *mp, TimeOutCB cb, void *cbdatap) :
				AsyncTimeout(mp), cb_(cb), cbdatap_(cbdatap) {
		}

		void timeoutExpired() noexcept {
			cb_(cbdatap_);
		}
	};

	disk(string path, uint16_t percent, vector<pair<uint32_t, uint8_t>> sizes,
			uint16_t iodepth, uint64_t runtime);
	~disk();
	void switchIOMode();
	int  verify();

	void writeDone(uint64_t sector, uint16_t nsectors);
	void readDone(const char *const bufp, uint64_t sector, uint16_t nsectors);
	int  iosSubmit(uint64_t nios);
//	void print_ios(void);

	uint64_t total_ios(void) {
		return ios.size();
	}
	uint64_t nsectors() {
		return sectors_;
	}
	uint64_t ioNSectors() {
		return sectors_ * percent_ / 100;
	}

	int disk_fd(void) {
		return fd;
	}

	void runtimeExpired();
	bool runInEventBaseThread(folly::Function<void()>);
private:
	IOMode                     mode_;
	unique_ptr<TimeoutWrapper> ioModeSwitchTimer_;
	bool                       modeSwitched_;

	uint64_t                   runtime_;
	unique_ptr<TimeoutWrapper> runtimeTimer_;
	bool                       runtimeComplete_ = false;

public: /* some test APIs */
	void cleanupEverything();
	void testReadSubmit(uint64_t s, uint16_t ns);
	void testWriteSubmit(uint64_t s, uint16_t ns);
	void testWriteOnceReadMany();
	void testOverWrite();
	void testBlockTrace(const string &file);
	void testNO1();
	void testNO2();
	void testNoOverlap();
	void testExactOverwrite();
	void testTailExactOverwrite();
	void testHeadExactOverwrite();
	void testDoubleSplit();
	void testTailOverwrite();
	void testHeadOverwrite();
	void testCompleteOverwrite();
	void testHeadSideSplit();
	void testMid();
	void testTailSideSplit();
	void testSectorReads();
	void _testSectorReads(uint64_t sector, uint16_t nsectors);
	void test();
};

#endif
