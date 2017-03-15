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

static inline uint64_t sector_to_byte(uint32_t sector)
{
	return sector << 9;
}

static inline uint32_t bytes_to_sector(uint64_t bytes)
{
	return bytes >> 9;
}

IO::IO(uint64_t sect, uint32_t nsec, string &pattern, int16_t pattern_start, IOType type):
	r(sect, nsec)
{
	this->pattern       = pattern;
	this->pattern_start = pattern_start;
	this->buffer        = {};
	this->type          = type;
}

size_t IO::size() {
	return sector_to_byte(r.nsectors);
}

uint64_t IO::offset() {
	return sector_to_byte(r.sector);
}

#define PAGE_SIZE 4096
#define DEFAULT_ALIGNMENT PAGE_SIZE

ManagedBuffer alignedAlloc(size_t sz) {
	void *bufp{};
	auto rc    = posix_memalign(&bufp, DEFAULT_ALIGNMENT,  sz);
	assert(rc == 0 && bufp != NULL);
	return ManagedBuffer(reinterpret_cast<char*>(bufp),
			[] (void *p) { free(p); });
}

ManagedBuffer IO::get_io_buffer() {
	assert(this->buffer == nullptr);

	auto sz = size();

	auto bufp = alignedAlloc(sz);
	char *bp  = bufp.get();

	/* fill pattern in the buffer */
	auto len = pattern.length();
	auto i   = sz / len;
	while (i--) {
		/* fast path */
		std::memcpy(bp, static_cast<const void *>(pattern.c_str()), len);
		bp += len;
	}

	i = sz - ((sz/len) * len);
	for (auto j = 0; j < i; j++) {
		/* slow char by char copy */
		*bp = pattern.at(j);
		bp++;
	}

	this->buffer = bufp;
	return bufp;
}

disk::disk(string path, vector<pair<uint32_t, uint8_t>> sizes)
{
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

	this->iodepth = 32;
	this->path    = path;
	this->size    = sz;
	this->sectors = bytes_to_sector(sz);
	this->ioevfd  = -1;

	iogen = make_shared<io_generator>(0, this->sectors, sizes);
}

disk::~disk()
{
	if (this->fd > 0) {
		close(this->fd);
	}

	if (this->ioevfd < 0) {
		close(this->ioevfd);
		ioevfd = -1;
	}

	io_destroy(this->context);
}

void disk::pattern_create(uint64_t sect, uint16_t nsec, string &pattern)
{
	pattern = "<" + std::to_string(sect) + "," + std::to_string(nsec) + ">";
}

void disk::io_done(vector<IOPtr> newiosp)
{
	std::lock_guard<std::mutex> l(this->lock);
	for (auto &newiop : newiosp) {
		assert(newiop->buffer);
		switch (newiop->type) {
		case IOType::WRITE:
			range r(newiop->r);
			write_done(r.sector, r.nsectors, newiop->pattern, newiop->pattern_start);
			break;
		case IOType::READ:
			read_done(newiop);
			break;
		}
	}
}

void disk::write_done(uint64_t sector, uint16_t nsectors, string pattern, int16_t pattern_start) {
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
		//assert(!io->get_buffer());

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

				write_done(oios, ons, opattern, 0);
			} else {
				if (oios != nios) {
					auto o1s  = oios;
					auto o1ns = nios - o1s;
					auto o1b  = opattern;

					assert(o1s + o1ns == nios);
					write_done(o1s, o1ns, opattern, 0);
				}

				auto o2s   = nioe + 1;
				auto d     = o2s - oios;
				auto o2ns  = oions - d;
				int16_t ps = sector_to_byte(d) % opattern.size();
				write_done(o2s, o2ns, opattern, ps);
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
			write_done(ss, ns, opattern, ps);
		}
	} while (1);
}

int disk::submit_writes(uint64_t nwrites) {
	struct iocb *ios[nwrites];
	struct iocb cbs[nwrites];

	uint64_t s;
	uint64_t ns;
	size_t   size;
	for (auto i = 0; i < nwrites; i++) {
		iogen->next_io(&s, &ns);
		assert(ns >= 1 && s <= sectors && s+ns <= sectors);

		string p;
		pattern_create(s, ns, p);

		auto iop = new IO(s, ns, p, 0, IOType::WRITE);
		auto cbp = &cbs[i];
		auto buf = iop->get_io_buffer();
		io_prep_pwrite(cbp, fd, buf.get(), iop->size(), iop->offset());
		io_set_eventfd(cbp, ioevfd);
		cbp->data = static_cast<void *>(iop);
		ios[i]    = cbp;
	}

	auto rc = io_submit(context, nwrites, ios);
	assert(rc == nwrites);
	if (rc < 0) {
		throw runtime_error("io_submit failed " + string(strerror(-rc)));
	}
	return 0;
}

int disk::submit_ios(uint64_t nios) {
	int rc;

	switch (mode) {
	case IOMode::WRITE:
		rc = submit_writes(nios);
		break;
	case IOMode::VERIFY:
		rc = submit_reads(nios);
		break;
	}
	return rc;
}

ssize_t io_result(struct io_event *ep) {
	return ((ssize_t)(((uint64_t)ep->res2 << 32) | ep->res));
}

void on_io_complete(evutil_socket_t fd, void *arg) {
	assert(arg != NULL);

	auto d = static_cast<disk *>(arg);
	assert(d->get_io_eventfd() == fd);

	eventfd_t nevents;
	while (1) {
		nevents = 0;
		int rc = eventfd_read(fd, &nevents);
		if (rc < 0 || nevents == 0) {
			if (rc < 0 && errno != EAGAIN) {
				cout << "rc " << rc << " nevents " << nevents << endl;
			}
			break;
		}

		struct io_event *eventsp = new struct io_event[nevents];
		rc = io_getevents(d->get_aio_context(), nevents, nevents, eventsp, NULL);
		assert(rc == nevents);
		unique_ptr<io_event[]> eventsup(eventsp);

		/* process IO completion */
		auto fu = std::async([d, eventsupCap = std::move(eventsup), nevents]() mutable {
			struct io_event *eventsp = eventsupCap.get();
			vector<IOPtr> iossp;
			for (auto ep = eventsp; ep < eventsp + nevents; ep++) {
				auto iop = static_cast<IO *>(ep->data);
				assert(iop->size() == io_result(ep));
				IOPtr iosp(iop);
				iossp.emplace_back(iosp);
			}
			d->io_done(iossp);
		});

		/* submit new set of IOs */
		rc = d->submit_ios(nevents);
		if (rc < 0) {
			cout << "submit_writes failed.\n";
		}

		fu.wait();
	}
}

class EventFDHandler : public EventHandler {
private:
	disk      *diskp_;
	int       fd_;
	EventBase *basep_;
public:
	EventFDHandler(disk *diskp, int fd, EventBase *basep) :
			EventHandler(basep, fd), diskp_(diskp), fd_(fd), basep_(basep) {
		assert(diskp && eventfd >= 0 && basep);
	}

	void handlerReady(uint16_t events) noexcept {
		assert(events & EventHandler::READ);
		if (events & EventHandler::READ) {
			on_io_complete(fd_, diskp_);
		}
	}
};

void disk::set_io_mode(IOMode mode) {
	this->mode_   = mode;
	this->timeout = std::make_unique<TimeoutWrapper>(this, &base);
	this->timeout->scheduleTimeout(MIN_TO_MILLI(1));
}

void disk::switch_io_mode() {
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
		dpCap->set_io_mode(mode);
	});
}

int disk::verify() {
	this->ioevfd = eventfd(0, EFD_NONBLOCK);
	if (ioevfd < 0) {
		throw runtime_error("eventfd creation failed " +
				string(strerror(errno)));
	}

	EventFDHandler handler(this, ioevfd, &base);
	handler.registerHandler(EventHandler::READ | EventHandler::PERSIST);

	std::memset(&this->context, 0, sizeof(this->context));
	auto rc = io_setup(iodepth * 2, &this->context);
	if (rc < 0) {
		throw runtime_error("io_setup failed " + string(strerror(-rc)));
	}

	set_io_mode(IOMode::WRITE);
	rc = submit_writes(iodepth);
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
