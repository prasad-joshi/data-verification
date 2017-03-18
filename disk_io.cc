#include <iostream>
#include <string>
#include <memory>
#include <future>
#include <set>
#include <vector>
#include <atomic>
#include <utility>
#include <algorithm>

#include <cstdlib>
#include <cstring>
#include <cassert>

#include <unistd.h>
#include <linux/fs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <event.h>
#include <sys/eventfd.h>
#include <libaio.h>

#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>
#include <folly/io/async/AsyncTimeout.h>

#include "disk_io.h"

using std::string;
using std::unique_ptr;
using std::shared_ptr;
using std::make_shared;
using std::cout;
using std::endl;
using std::runtime_error;

using namespace folly;

static inline uint64_t sector_to_byte(uint32_t sector) {
	return sector << 9;
}

static inline uint32_t bytes_to_sector(uint64_t bytes) {
	return bytes >> 9;
}

IO::IO(uint64_t sect, uint32_t nsec, const string &pattern, int16_t pattern_start):
			r(sect, nsec) {
	this->pattern       = pattern;
	this->pattern_start = pattern_start;
}

size_t IO::size() {
	return sector_to_byte(r.nsectors);
}

uint64_t IO::offset() {
	return sector_to_byte(r.sector);
}

disk::disk(string path, vector<pair<uint32_t, uint8_t>> sizes, uint16_t qdepth) : asyncio(qdepth) {
	fd = open(path.c_str(), O_RDWR | O_DIRECT);
	if (fd < 0) {
		throw runtime_error("Could not open file " + path);
	}

	struct stat sb;
	auto rc = fstat(fd, &sb);
	if (rc < 0 || !S_ISBLK(sb.st_mode)) {\
		throw runtime_error(path + " is not a block device.");
	}
	assert(rc >= 0 && S_ISBLK(sb.st_mode));

	uint64_t sz;
	rc = ioctl(fd, BLKGETSIZE64, &sz);
	if (rc < 0 || sz == 0) {
		throw runtime_error("unable to find size of device " + path);
	}
	assert(sz != 0);

	this->iodepth = qdepth;
	this->path    = path;
	this->size    = sz;
	this->sectors = bytes_to_sector(sz);
	this->iogen   = std::make_unique<io_generator>(0, this->sectors, sizes);
}

disk::~disk() {
	if (this->fd > 0) {
		close(this->fd);
	}
}

void disk::patternCreate(uint64_t sect, uint16_t nsec, string &pattern) {
	pattern = "<" + std::to_string(sect) + "," + std::to_string(nsec) + ">";
}

ManagedBuffer disk::prepareIOBuffer(size_t size, const string &pattern) {
	auto bufp = asyncio.getIOBuffer(size);
	assert(bufp);

	char *bp = bufp.get();

	/* fill pattern in the buffer */
	auto len = pattern.length();
	auto i   = size / len;
	while (i--) {
		/* fast path */
		std::memcpy(bp, static_cast<const void *>(pattern.c_str()), len);
		bp += len;
	}

	i = size - ((size/len) * len);
	for (auto j = 0; j < i; j++) {
		/* slow char by char copy */
		*bp = pattern.at(j);
		bp++;
	}

	return std::move(bufp);
}

ManagedBuffer disk::getIOBuffer(size_t size) {
	return asyncio.getIOBuffer(size);
}

int disk::writesSubmit(uint64_t nwrites) {
	struct iocb *ios[nwrites];
	struct iocb cbs[nwrites];
	struct iocb *cbp;
	uint64_t    s;
	uint64_t    ns;
	size_t      sz;
	uint64_t    o;

	for (auto i = 0; i < nwrites; i++) {
		iogen->next_io(&s, &ns);
		assert(ns >= 1 && s <= sectors && s+ns <= sectors);

		string p;
		patternCreate(s, ns, p);

		sz        = sector_to_byte(ns);
		o         = sector_to_byte(s);
		auto bufp = prepareIOBuffer(sz, p);
		cbp       = &cbs[i];
		ios[i]    = cbp;
		asyncio.pwritePrepare(cbp, fd, std::move(bufp), sz, o);
	}

	auto rc = asyncio.pwrite(ios, nwrites);
	assert(rc == nwrites);
	if (rc < 0) {
		throw runtime_error("io_submit failed " + string(strerror(-rc)));
	}
	return 0;
}

int disk::readsSubmit(uint64_t nreads) {
	struct iocb *ios[nreads];
	struct iocb cbs[nreads];
	struct iocb *cbp;
	uint64_t    s;
	uint64_t    ns;
	size_t      sz;
	uint64_t    o;
	
	for (auto i = 0; i < nreads; i++) {
		iogen->next_io(&s, &ns);
		assert(ns >= 1 && s <= sectors && s+ns <= sectors);

		sz        = sector_to_byte(ns);
		o         = sector_to_byte(s);
		auto bufp = getIOBuffer(sz);
		cbp       = &cbs[i];
		ios[i]    = cbp;
		asyncio.preadPrepare(cbp, fd, std::move(bufp), sz, o);
	}

	auto rc = asyncio.pread(ios, nreads);
	assert(rc == nreads);
	if (rc < 0) {
		throw runtime_error("io_submit failed " + string(strerror(-rc)));
	}
	return 0;
}

int disk::iosSubmit(uint64_t nios) {
	int rc;

	switch (mode_) {
	case IOMode::WRITE:
		rc = writesSubmit(nios);
		break;
	case IOMode::VERIFY:
		rc = readsSubmit(nios);
		break;
	}
	return rc;
}

void disk::readDone(const char *const data, uint64_t sector, uint16_t nsectors, const string &pattern) {
}

void disk::writeDone(uint64_t sector, uint16_t nsectors, const string &pattern, const int16_t pattern_start) {
	range r(sector, nsectors);
	auto nios = r.start_sector(); /* new IO start sector */
	auto nioe = r.end_sector();   /* new IO end sector   */

	do {
		auto io = ios.find(r);
		if (io == ios.end()) {
			auto newiop = make_shared<IO>(sector, nsectors, pattern, pattern_start);
			ios.insert(newiop);
			break;
		}

		/* overlapping IO range found */
		/*
		std::cout << "Trying to insert " << sector << " " << nsectors << " ";
		std::cout << "preset " << (*io)->r.sector << " " << (*io)->r.nsectors << std::endl;
		*/

		auto oios     = (*io)->r.start_sector(); /* old IO start sector */
		auto oions    = (*io)->r.nsectors;       /* old IO nsectors */
		auto oioe     = (*io)->r.end_sector();   /* old IO end sector   */
		auto opattern = (*io)->pattern;
		if (oios == nios && oioe == nioe) {
			/* exact match - only update pattern */
			(*io)->pattern       = pattern;
			(*io)->pattern_start = 0;
			break;
		}

		ios.erase(io);
		if (nios <= oios && nioe >= oioe) {
			/*
			 * old is too small - only delete it 
			 *
			 * For example:
			 * New IO --> sector 100, nsectors 8
			 * Old IO --> sector 101, nsectors 2
			 *
			 * The new IO completely overrides old IO
			 * */
			continue;
		}

		if (oios <= nios) {
			if (oioe <= nioe) {
				/*
				 * old IO sector < new IO sector and old IO end <= new IO end
				 *
				 * for example:
				 * CASE1: OLD IO (16, 16) and NEW IO(24, 16)
				 *                      16       31
				 *               OLD IO |--------|
				 *                           24        39
				 *               NEW IO      |---------|
				 *
				 * CASE2: OLD IO (32, 32) and NEW IO (56, 8)
				 *                      32               63
				 *               OLD IO |----------------|
				 *                                 56    63
				 *               NEW IO            |-----|
				 *
				 * In both the case old io's end is modified and new io is inserted
				 */
				oioe     = nios - 1;
				auto ons = oioe - oios + 1;
				assert(ons != 0);

				writeDone(oios, ons, opattern, 0);
			} else {
				if (oios != nios) {
					auto o1s  = oios;
					auto o1ns = nios - o1s;
					auto o1b  = opattern;

					assert(o1s + o1ns == nios);
					writeDone(o1s, o1ns, opattern, 0);
				}

				auto o2s   = nioe + 1;
				auto d     = o2s - oios;
				auto o2ns  = oions - d;
				int16_t ps = sector_to_byte(d) % opattern.size();
				writeDone(o2s, o2ns, opattern, ps);
			}
		} else {
			/*
			 * old IO ==> sector 32, nsectors 16 i.e. sectors 32 to 47
			 * new IO ==> sector 24, nsectors 16 i.e. sectors 24 to 39
			 *
			 * change old IO's start to 40 and nsectors to 8
			 * old IO's pattern remains as it is, however pattern start may change
			 */
			auto d = r.end_sector() - oios + 1;
			assert(d != 0);
			auto ns = oions - d; /* calculate nsectors */
			auto ss = oios + d;  /* start sector number */
			assert(r.sector + r.nsectors == ss);
			int16_t ps = sector_to_byte(d) % opattern.size(); /* pattern start */
			writeDone(ss, ns, opattern, ps);
		}
	} while (1);
}

void ioCompleted(void *cbdata, ManagedBuffer bufp, size_t size, uint64_t offset, ssize_t result, bool read) {
	assert(cbdata && bufp && result == size);

	disk     *diskp   = reinterpret_cast<disk *>(cbdata);
	uint64_t sector   = bytes_to_sector(offset);
	uint32_t nsectors = bytes_to_sector(size);
	char     *bp      = bufp.get();
	string   pattern;

	diskp->patternCreate(sector, nsectors, pattern);
	if (read == true) {
		diskp->readDone(bp, sector, nsectors, pattern);
	} else {
		diskp->writeDone(sector, nsectors, pattern, 0);
	}

	diskp->iosSubmit(1);
}

void disk::setIOMode(IOMode mode) {
	this->mode_   = mode;
	this->timeout = std::make_unique<TimeoutWrapper>(this, &base);
	this->timeout->scheduleTimeout(MIN_TO_MILLI(1));
}

void disk::switchIOMode() {
	IOMode m;
	switch (this->mode_) {
	case IOMode::WRITE:
		m = IOMode::VERIFY;
		cout << "Setting IO Mode to VERIFY\n";
		break;
	case IOMode::VERIFY:
		m = IOMode::WRITE;
		cout << "Setting IO Mode to WRITE\n";
		break;
	}

	base.runInEventBaseThread([dpCap = this, mode = m] () {
		dpCap->setIOMode(mode);
	});
}

int disk::verify() {
	asyncio.init(&base);
	asyncio.registerIOCompleteCB(ioCompleted, this);

	setIOMode(IOMode::WRITE);
	auto rc = iosSubmit(iodepth);
	assert(rc == 0);

	base.loopForever();
	// rc = event_base_dispatch(ebp);
	cout << "rc = " << rc << endl;
}
#if 0
void disk::print_ios(void)
{
	bool  first = false;
	range r;

	for (auto &w: ios) {
		// std::cout << w->r.sector << " " << w->r.nsectors << " " << w->buffer << std::endl;
		if (first == false) {
			first = true;
			r = w->r;
		} else {
			auto e = r.end_sector();
			assert(e < w->r.sector);
			r = w->r;
		}
	}
}
#endif