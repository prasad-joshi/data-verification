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

#include "disk_io.h"

using std::string;
using std::unique_ptr;
using std::shared_ptr;
using std::make_shared;
using std::cout;
using std::endl;

static inline uint64_t sector_to_byte(uint32_t sector)
{
	return sector << 9;
}

static inline uint32_t bytes_to_sector(uint64_t bytes)
{
	return bytes >> 9;
}

IO::IO(uint64_t sect, uint32_t nsec, string &pattern, int16_t pattern_start):
	r(sect, nsec)
{
	this->pattern       = pattern;
	this->pattern_start = pattern_start;
	this->buffer        = nullptr;
}

size_t IO::size() {
	return sector_to_byte(r.nsectors);
}

uint64_t IO::offset() {
	return sector_to_byte(r.sector);
}

std::shared_ptr<char> IO::get_buffer()
{
	assert(this->buffer == nullptr);

	auto sz = size();

	char *bp = new char[sz];
	if (bp == nullptr) {
		return nullptr;
	}

	/* create shared pointer to return */
	std::shared_ptr<char> rbp(bp, [] (char *p) { delete[] p; });

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

	/* maintain buffer pointer */
	this->buffer = rbp;
	return rbp;
}

disk::disk(string path, vector<pair<uint32_t, uint8_t>> sizes)
{
	fd = open(path.c_str(), O_RDWR | O_DIRECT);
	if (fd < 0) {
		return;
	}

	uint64_t sz;
	auto     rc = ioctl(fd, BLKGETSIZE64, &sz);
	if (rc < 0) {
		return;
	}

	this->iodepth = 32;
	this->path    = path;
	this->size    = sz;
	this->sectors = bytes_to_sector(sz);
	this->ebp     = NULL;
	this->ioevfd  = -1;
	this->ioevp   = NULL;

	iogen = make_shared<io_generator>(0, this->sectors, sizes);
}

disk::~disk()
{
	if (this->fd > 0) {
		close(this->fd);
	}

	if (this->ioevp != NULL) {
		event_del(this->ioevp);
		event_free(this->ioevp);
		this->ioevp = NULL;
	}

	if (this->ebp != NULL) {
		event_base_free(this->ebp);
		this->ebp = NULL;
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

void disk::write_done(IOPtr newiop)
{
	assert(newiop->buffer == NULL);
	range r(newiop->r.sector, newiop->r.nsectors);
	auto nios = r.start_sector(); /* new IO start sector */
	auto nioe = r.end_sector();   /* new IO end sector   */

	do {
		auto io = ios.find(r);
		if (io == ios.end()) {
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
			(*io)->pattern       = newiop->pattern;
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

				auto iop = make_shared<IO>(oios, ons, opattern, 0);
				write_done(iop);
			} else {
				if (oios != nios) {
					auto o1s  = oios;
					auto o1ns = nios - o1s;
					auto o1b  = opattern;

					assert(o1s + o1ns == nios);
					auto iop = make_shared<IO>(o1s, o1ns, opattern, 0);
					write_done(iop);
				}

				auto o2s  = nioe + 1;
				auto d    = o2s - oios;
				auto o2ns = oions - d;
				auto o2b  = (*io)->buffer + (d * 512);
				write_done(o2s, o2ns, o2b);
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
			auto iop = make_shared<IO>(ss, ns, opattern, ps);
			write_done(iop);
		}
	} while (1);
}

int disk::submit_writes(uint64_t nwrites) {
	struct iocb cbs[nwrites];

	uint64_t s;
	uint64_t ns;
	size_t   size;
	for (auto i = 0; i < nwrites; i++) {
		iogen->next_io(&s, &ns);
		assert(ns >= 1);

		string p;
		pattern_create(s, ns, p);

		auto iop = new IO(s, ns, p, 0);
		auto cbp = &cbs[i];
		auto buf = iop->get_buffer();
		io_prep_pwrite(cbp, fd, (void *) buf.get(), iop->size(), iop->offset());
		io_set_eventfd(cbp, ioevfd);
		cbp->data = static_cast<void *>(iop);
	}

	return io_submit(this->get_aio_context(), nwrites, (iocb **) cbs);
}

size_t io_result(struct io_event *ep) {
	return ((ssize_t)(((uint64_t)ep->res2 << 32) | ep->res));
}

void on_io_complete(evutil_socket_t fd, short what, void *arg) {
	assert(arg != NULL);

	auto d = static_cast<disk *>(arg);
	assert(d->get_io_eventfd() == fd && d->get_io_event());

	eventfd_t nevents;
	int rc = eventfd_read(d->disk_fd(), &nevents);
	if (rc < 0 || nevents == 0) {
		event_base_loopbreak(d->get_event_base());
		return;
	}

	struct io_event *eventsp = new struct io_event[nevents];
	rc = io_getevents(d->get_aio_context(), nevents, nevents, eventsp, NULL);
	assert(rc == nevents);
	unique_ptr<io_event[]> eventsup(eventsp);

	/* process IO completion */
	auto fu = std::async(std::launch::deferred, 
			[d, eventsupCap = std::move(eventsup), nevents]() {
		struct io_event *eventsp = eventsupCap.get();
		for (auto ep = eventsp; ep < eventsp + nevents; ep++) {
			auto iop = static_cast<IO *>(ep->data);
			assert(iop->size() == io_result(ep));
			/* free buffer pointer - it is no longer required */
			iop->buffer = nullptr;

			IOPtr iosp(iop);
			d->write_done(iosp);
		}
	});

	/* submit new set of IOs */
	rc = d->submit_writes(nevents);
	if (rc < 0) {
		event_base_loopbreak(d->get_event_base());
	}

	fu.wait();
}

int disk::verify() {
	this->ebp = event_base_new();
	if (this->ebp == NULL) {
		std::cerr << "libevent initialization failed.\n";
		return -1;
	}

	this->ioevfd = eventfd(0, EFD_NONBLOCK);
	if (ioevfd < 0) {
		std::cerr << "eventfd: %s\n" << strerror(errno);
		return -1;
	}

	this->ioevp = event_new(ebp, ioevfd, EV_READ, on_io_complete, this);
	if (ioevp == NULL) {
		std::cerr << "event_new failed.\n";
		return -1;
	}
	assert(ioevp != NULL);

	auto rc = io_setup(iodepth * 2, &this->context);
	if (rc < 0) {
		std::cerr << "io_setup: %s\n" << strerror(errno);
		return -1;
	}

	event_add(ioevp, NULL);
	event_base_dispatch(ebp);
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