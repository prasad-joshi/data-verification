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

static inline uint64_t sector_to_byte(uint64_t sector) {
	return sector << 9;
}

static inline uint64_t bytes_to_sector(uint64_t bytes) {
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
	modeSwitched_ = false;
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
	const char *const p = pattern.c_str();
	auto len = pattern.length();
	auto i   = size / len;
	assert(i > 0);
	while (i--) {
		std::memcpy(bp, p, len);
		bp += len;
	}

	i = size - ((size/len) * len);
	std::memcpy(bp, p, i);
	return std::move(bufp);
}

ManagedBuffer disk::getIOBuffer(size_t size) {
	return asyncio.getIOBuffer(size);
}

void disk::addWriteIORange(uint64_t sector, uint16_t nsectors) {
	range nr(sector, nsectors);
	bool  c = true;

	for (auto &it : writeIOsSubmitted) {
		if (nr < it.first || it.first < nr) {
			/* not equal */
			continue;
		}
		c         = false;
		it.second = false;
	}

	pair<range, bool> p(nr, c);
	writeIOsSubmitted.push_back(p);
}

pair<range, bool> disk::removeWriteIORange(uint64_t sector, uint16_t nsectors) {
	range             nr(sector, nsectors);
	pair<range, bool> res;
	bool              found = false;

	auto it = writeIOsSubmitted.begin();
	while (it != writeIOsSubmitted.end()) {
		if (nr < it->first || it->first < nr) {
			it++;
			continue;
		}
		res = *it;
		writeIOsSubmitted.erase(it);
		found = true;
		break;
	}
	assert(found == true);
	return res;
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

		// cout << "W " << s << " " << ns << endl;

		string p;
		patternCreate(s, ns, p);

		sz        = sector_to_byte(ns);
		o         = sector_to_byte(s);
		auto bufp = prepareIOBuffer(sz, p);
		cbp       = &cbs[i];
		ios[i]    = cbp;
		asyncio.pwritePrepare(cbp, fd, std::move(bufp), sz, o);
		addWriteIORange(s, ns);
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

		// cout << "R " << s << " " << ns << endl;
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

	if (modeSwitched_ == true) {
		/* wait till all submitted IOs are complete */
		if (asyncio.getPending() != 0) {
			return 0;
		}

		// cout << "No pending IOs.\n";
		nios = iodepth;
		modeSwitched_ = false;
		// sleep(10);
	}

	assert(modeSwitched_ == false);
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

bool disk::patternCompare(const char *const bufp, size_t size, const string &pattern, int16_t start) {
	assert(bufp && pattern.length() && pattern.length() >= start &&
			size > pattern.length() && size >= 512 && pattern.length() < 512);

	const char *const p = pattern.c_str();
	const char *bp      = bufp;
	auto len = pattern.length();
	auto i   = size / len;
	assert(i > 0);
	while (i--) {
		/* fast path */
		auto cl = len - start;
		auto rc = std::memcmp(bp, p + start, cl);
		if (rc != 0) {
			/* corruption */
			assert(0);
			return true;
		}
		bp    += cl;
		start  = 0;
	}

	i = size - ((size/len) * len);
	auto rc = std::memcmp(bp, p, i);
	if (rc != 0) {
		/* corruption */
		return true;
	}
	return false;
}

bool disk::readDataVerify(const char *const data, uint64_t sector, uint16_t nsectors) {
	range r(sector, nsectors);
	auto  io = ios.find(r);
	if (io == ios.end()) {
		/* case 0: <sector, nsector> are never written before */
		return false;
	}

	auto rios = r.start_sector();
	auto rioe = r.end_sector();
	auto oios = (*io)->r.start_sector();
	auto oioe = (*io)->r.end_sector();

	const char *vbufp = data;     /* verification buffer pointer */
	auto       vssec  = sector;   /* verfification start sector  */
	auto       vnsec  = nsectors; /* number of sectors to verify */
	if (rios < oios) {
		/*
		 * CASE 1:
		 *
		 * Read IOs start sector is before found IOs start sector. Since this
		 * part is not part of IOS set, it should never have data corruption.
		 */
		auto s  = rios;
		auto ns = oios - s;
		auto c  = readDataVerify(vbufp, s, ns);
		if (c == true) {
			/* corruption */
			assert(0);
		}
		assert(vnsec > ns);

		vbufp += sector_to_byte(ns);
		vssec  = oios;
		vnsec  = vnsec - ns;
	}
	assert(vnsec > 0);

	/*
	 * CASE 2:
	 *
	 */
	auto s    = vssec;
	auto vend = MIN(oioe, rioe);
	auto ns   = vend - s + 1;
	assert(ns <= vnsec);
	auto d    = vssec - oios;
	auto ps   = (sector_to_byte(d) + (*io)->pattern_start) % (*io)->pattern.length();
	auto c    = patternCompare(vbufp, sector_to_byte(ns), (*io)->pattern, ps);
	if (c == true) {
		/* corruption */
		cout << "Read IO (" << sector << ", " << nsectors << ") corruption at (" << s << "," << ns << ")" << endl;
		assert(0);
		return true;
	} else if (vnsec <= ns) {
		/* data has been verified - no corruption */
		assert(vnsec - ns == 0);
		return false;
	}

	/* CASE 3: */
	vssec  = vend + 1;
	vnsec -= ns;
	vbufp += sector_to_byte(ns);

	return readDataVerify(vbufp, vssec, vnsec);
}

void disk::readDone(const char *const bufp, uint64_t sector, uint16_t nsectors) {
	auto corruption = readDataVerify(bufp, sector, nsectors);
	if (corruption) {
		assert(0);
	}
}

void disk::writeDone(uint64_t sector, uint16_t nsectors) {
	auto pr = removeWriteIORange(sector, nsectors);
	if (pr.second == false) {
		/*
		 * TODO: improve this
		 *
		 * IO on same range of sectors was submitted simultaneously. There is
		 * no simple way to verify data. At the moment, we remove all traces
		 * of the related IOs.
		 */
		while (1) {
			auto io = ios.find(pr.first);
			if (io == ios.end()) {
				break;
			}
			ios.erase(io);
		}
		assert(ios.find(pr.first) == ios.end());
		return;
	}

	string p;
	patternCreate(sector, nsectors, p);
	writeDone(sector, nsectors, p, 0);
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
		auto ops      = (*io)->pattern_start;
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

				writeDone(oios, ons, opattern, ops);
			} else {
				if (oios != nios) {
					auto o1s  = oios;
					auto o1ns = nios - o1s;
					auto o1b  = opattern;

					assert(o1s + o1ns == nios);
					writeDone(o1s, o1ns, opattern, ops);
				}

				auto o2s   = nioe + 1;
				auto d     = o2s - oios;
				auto o2ns  = oions - d;
				int16_t ps = (sector_to_byte(d) + ops) % opattern.size();
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
			int16_t ps = (sector_to_byte(d) + ops) % opattern.size(); /* pattern start */
			writeDone(ss, ns, opattern, ps);
		}
	} while (1);
}

void ioCompleted(void *cbdata, ManagedBuffer bufp, size_t size, uint64_t offset, ssize_t result, bool read) {
	assert(cbdata && bufp && result == size && size >= sector_to_byte(1));

	disk     *diskp   = reinterpret_cast<disk *>(cbdata);
	uint64_t sector   = bytes_to_sector(offset);
	uint16_t nsectors = bytes_to_sector(size);
	assert(nsectors >= 1);

	if (read == true) {
		char *bp = bufp.get();
		diskp->readDone(bp, sector, nsectors);
	} else {
		diskp->writeDone(sector, nsectors);
	}
}

void nioCompleted(void *cbdata, uint16_t nios) {
	assert(cbdata);

	disk *diskp = reinterpret_cast<disk *>(cbdata);
	diskp->iosSubmit(nios);
}

bool disk::runInEventBaseThread(folly::Function<void()> func) {
	return base.runInEventBaseThread(std::move(func));
}

void disk::setIOMode(IOMode mode) {
	this->mode_   = mode;
	this->timeout = std::make_unique<TimeoutWrapper>(this, &base);
	this->timeout->scheduleTimeout(MIN_TO_MILLI(5));
}

void disk::switchIOMode() {
	assert(modeSwitched_ == false);

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
	modeSwitched_ = true;

	base.runInEventBaseThread([dpCap = this, mode = m] () {
		dpCap->setIOMode(mode);
	});
}

int disk::verify() {
	asyncio.init(&base);
	asyncio.registerCallback(ioCompleted, nioCompleted,this);

	setIOMode(IOMode::WRITE);
	auto rc = iosSubmit(iodepth);
	assert(rc == 0);

	base.loopForever();
	// rc = event_base_dispatch(ebp);
}

void disk::cleanupEverything() {
	ios.erase(ios.begin(), ios.end());
}

void disk::testReadSubmit(uint64_t s, uint16_t ns) {
	struct iocb *ios[1];
	struct iocb cb;
	size_t      sz;
	uint64_t    o;
	
	sz        = sector_to_byte(ns);
	o         = sector_to_byte(s);
	auto bufp = getIOBuffer(sz);
	ios[0]    = &cb;
	asyncio.preadPrepare(&cb, fd, std::move(bufp), sz, o);

	auto rc = asyncio.pread(ios, 1);
	assert(rc == 1);
}

void disk::testWriteSubmit(uint64_t s, uint16_t ns) {
	struct iocb *ios[1];
	struct iocb cb;
	size_t      sz;
	uint64_t    o;

	string p;
	patternCreate(s, ns, p);

	sz        = sector_to_byte(ns);
	o         = sector_to_byte(s);
	auto bufp = prepareIOBuffer(sz, p);
	ios[0]    = &cb;
	asyncio.pwritePrepare(&cb, fd, std::move(bufp), sz, o);

	auto rc = asyncio.pwrite(ios, 1);
	assert(rc == 1);
}

void disk::testWriteOnceReadMany() {
	testWriteSubmit(512, 16);
	base.loopOnce();
	for (auto ns = 50; ns != 0; ns--) {
		testReadSubmit(500, ns);
		base.loopOnce();
	}

	testWriteSubmit(100, 9);
	base.loopOnce();
	for (auto ns = 50; ns != 0; ns--) {
		testReadSubmit(100, ns);
		base.loopOnce();
	}
	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testOverWrite() {
	cleanupEverything();
	assert(ios.size() == 0);
	for (auto step = 1; step < 16; step++) {
		for (int16_t ns = 16, c = 0; ns > 0; ns-=step, c++) {
			testWriteSubmit(512, ns);
			base.loopOnce();
			assert(ios.size() == c+1);
			testReadSubmit(500, 50);
			base.loopOnce();
		}
		cleanupEverything();
		assert(ios.size() == 0);
	}
	assert(ios.size() == 0);

	for (auto step = 1; step < 160; step++) {
		for (int16_t ns = 160, c = 0; ns > 0; ns-=step, c++) {
			testWriteSubmit(100, ns);
			base.loopOnce();
			assert(ios.size() == c+1);
			testReadSubmit(98, 200);
			base.loopOnce();
		}
		cleanupEverything();
		assert(ios.size() == 0);
	}
	assert(ios.size() == 0);
}

void disk::testNO2() {
/*
W 8081398 1404
W 8081398 909
W 8081398 1093
R 8082135 8
*/
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(8081398, 1404);
	base.loopOnce();
	assert(ios.size() == 1);
#if 0
	cout << "1\n";
	for (auto &io : ios) {
		cout << io->r.sector << " " << io->r.nsectors << " " << io->pattern << " " << io->pattern_start << endl;
	}
#endif

	testWriteSubmit(8081398, 909);
	base.loopOnce();
	assert(ios.size() == 2);

	testWriteSubmit(8081398, 1093);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(8082135, 8);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testNO1() {
	cleanupEverything();
	assert(ios.size() == 0);

	const uint64_t SECTOR = 1783797;

	testWriteSubmit(SECTOR, 1207);
	base.loopOnce();
	assert(ios.size() == 1);
	for (auto &io : ios) {
		string p;
		patternCreate(SECTOR, 1207, p);
		assert(io->r.sector == SECTOR && io->r.nsectors == 1207 &&
				io->pattern == p && io->pattern_start == 0);
	}

	auto ns = 8;
	auto sz = sector_to_byte(ns);
	testWriteSubmit(SECTOR, ns);
	base.loopOnce();
	assert(ios.size() == 2);
	int c = 0;
	for (auto &io : ios) {
		if (c == 0) {
			string p;
			patternCreate(SECTOR, ns, p);
			assert(io->r.sector == SECTOR && io->r.nsectors == ns &&
					io->pattern == p && io->pattern_start == 0);
		} else {
			string p;
			patternCreate(SECTOR, 1207, p);
			auto ps = sz % p.length();
			assert(io->r.sector == SECTOR+ns && io->r.nsectors == 1207-ns && 
					io->pattern == p && io->pattern_start == ps);
		}
		c++;
	}

	auto ns1 = 16;
	auto sz1 = sector_to_byte(ns1);
	testWriteSubmit(SECTOR, 16);
	base.loopOnce();
	assert(ios.size() == 2);
	c = 0;
	for (auto &io : ios) {
		if (c == 0) {
			string p;
			patternCreate(SECTOR, ns1, p);
			assert(io->r.sector == SECTOR && io->r.nsectors == ns1 &&
					io->pattern == p && io->pattern_start == 0);
		} else {
			string p;
			patternCreate(SECTOR, 1207, p);
			auto ps = sz1 % p.length();
			assert(io->r.sector == SECTOR+ns1 && io->r.nsectors == 1207-ns1 && 
					io->pattern == p && io->pattern_start == ps);
		}
		c++;
	}

	testReadSubmit(1783797, 64);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testNoOverlap() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 3000);
	base.loopOnce();

	testWriteSubmit(2000, 500);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(1000, 3000);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testExactOverwrite() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testTailExactOverwrite() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	testWriteSubmit(1300, 200);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(1000, 500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testHeadExactOverwrite() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	testWriteSubmit(1000, 100);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(1000, 500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testDoubleSplit() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	testWriteSubmit(1200, 100);
	base.loopOnce();
	assert(ios.size() == 3);
	testReadSubmit(1000, 500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testTailOverwrite() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	testWriteSubmit(1300, 500);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(1000, 1000);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testHeadOverwrite() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	testWriteSubmit(800, 500);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(500, 2000);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testCompleteOverwrite() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 100);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	testWriteSubmit(1000, 500);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(1000, 500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testHeadSideSplit() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 2000);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1000, 100);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1000, 200);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1300, 200);
	base.loopOnce();
	assert(ios.size() == 4);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1600, 600);
	base.loopOnce();
	assert(ios.size() == 6);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1400, 600);
	base.loopOnce();
	assert(ios.size() == 6);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1100, 20);
	base.loopOnce();
	assert(ios.size() == 8);
	testReadSubmit(800, 2500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testMid() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 2000);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1500, 50);
	base.loopOnce();
	assert(ios.size() == 3);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1300, 350);
	base.loopOnce();
	assert(ios.size() == 3);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1200, 500);
	base.loopOnce();
	assert(ios.size() == 3);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1600, 10);
	base.loopOnce();
	assert(ios.size() == 5);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1350, 300);
	base.loopOnce();
	assert(ios.size() == 5);
	testReadSubmit(800, 2500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::testTailSideSplit() {
	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(1000, 2000);
	base.loopOnce();
	assert(ios.size() == 1);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(2500, 500);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(2000, 1000);
	base.loopOnce();
	assert(ios.size() == 2);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(2200, 500);
	base.loopOnce();
	assert(ios.size() == 4);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(2200, 600);
	base.loopOnce();
	assert(ios.size() == 4);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(2100, 300);
	base.loopOnce();
	assert(ios.size() == 5);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1500, 300);
	base.loopOnce();
	assert(ios.size() == 7);
	testReadSubmit(800, 2500);
	base.loopOnce();

	testWriteSubmit(1700, 500);
	base.loopOnce();
	assert(ios.size() == 6);
	testReadSubmit(800, 2500);
	base.loopOnce();

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::_testSectorReads(uint64_t sector, uint16_t nsectors) {
	uint64_t s;
	uint16_t ns;
	for (s = sector, ns = 0; ns < nsectors; ns++, s++) {
		testReadSubmit(s, 1);
		base.loopOnce();
	}
}

void disk::testSectorReads() {
	const uint64_t SECTOR   = 1000;
	const uint16_t NSECTORS = 500;

	cleanupEverything();
	assert(ios.size() == 0);

	testWriteSubmit(SECTOR, NSECTORS);
	base.loopOnce();
	assert(ios.size() == 1);
	_testSectorReads(SECTOR, NSECTORS);

	for (auto s = SECTOR; s < (SECTOR + NSECTORS); s += 2) {
		testWriteSubmit(s, 1);
		base.loopOnce();
		_testSectorReads(SECTOR, NSECTORS);
	}

	cleanupEverything();
	assert(ios.size() == 0);
}

void disk::test() {
	asyncio.init(&base);
	asyncio.registerCallback(ioCompleted, nullptr, this);

	testWriteOnceReadMany();
	testOverWrite();
	testNO1();
	testNO2();
	testNoOverlap();
	testNoOverlap();
	testExactOverwrite();
	testTailExactOverwrite();
	testHeadExactOverwrite();
	testDoubleSplit();
	testTailOverwrite();
	testHeadOverwrite();
	testCompleteOverwrite();
	testHeadSideSplit();
	testMid();
	testTailSideSplit();
	testSectorReads();
}

void lineSplit(const string &line, const char delim, vector<string> &result) {
	std::stringstream ss;
	ss.str(line);
	string item;
	while (std::getline(ss, item, delim)) {
		result.emplace_back(item);
	}
}

#include <fstream>

void disk::testBlockTrace(const string &file) {
	asyncio.init(&base);
	asyncio.registerCallback(ioCompleted, nullptr, this);

	std::ifstream ifs(file);
	if (!ifs.is_open()) {
		return;
	}

	range tr(8082135, 8);
	string line;
	while (std::getline(ifs, line)) {
		std::vector<string> l;
		lineSplit(line, ' ', l);
		if (l.size() != 3) {
			cout << "Unabled to parse trace " << line << endl;
			continue;
		}

		uint64_t s  = std::stoul(l[1]);
		uint16_t ns = std::stoul(l[2]);
		range r(s, ns);

		if ((tr.sector >= s && tr.sector <= r.end_sector()) || (tr.end_sector() >= s && tr.end_sector() <= r.end_sector())) {
			if (l[0] == "W") {
				cout << "W " << s << " " << ns << endl;
				testWriteSubmit(s, ns);
			} else if (l[0] == "R") {
				cout << "R " << s << " " << ns << endl;
				testReadSubmit(s, ns);
			} else {
				cout << "Unrecognized operation " << l[0] << endl;
				continue;
			}
			base.loopOnce();
		}
	}
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