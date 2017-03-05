#include <iostream>
#include <string>
#include <memory>
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

#include "disk_io.h"

using std::string;

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
}

size_t IO::size() {
	return sector_to_byte(r.nsectors);
}

std::shared_ptr<char> IO::get_buffer()
{
	auto sz = size();

	char *bp = new char[sz];
	if (bp == nullptr) {
		*sizep = 0;
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

	return rbp;
}

disk::disk(string path)
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

	this->path    = path;
	this->size    = sz;
	this->sectors = bytes_to_sector(sz);
	this->eb      = NULL;
	this->ioevfd  = -1;
	this->ioev    = NULL;
}

disk::~disk()
{
	if (this->fd > 0) {
		close(this->fd);
	}

	if (this->ioev != NULL) {
		event_free(this->ioev);
		this->ioev = NULL;
	}

	if (this->eb != NULL) {
		event_base_free(this->eb);
		this->eb = NULL;
	}

	if (this->ioevfd < 0) {
		close(this->ioevfd);
		ioevfd = -1;
	}

	io_destroy(this->context);
}

void disk::pattern_create(uint64_t sector, uint16_t nsectors, string &pattern)
{
	pattern = "<" + std::to_string(sect) + "," + std::to_string(nsec) + ">";
}

void disk::write_done(IOPtr *newiop)
{
	range r(newiop->sector, newiop->nsectors);
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

		auto oios = (*io)->r.start_sector(); /* old IO start sector */
		auto oioe = (*io)->r.end_sector();   /* old IO end sector   */
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
				oioe     = nios - 1;
				auto ons = oioe - oios + 1;
				assert(ons != 0);

				auto iop = std::make_shared<IO>(oios, ons, opattern, 0);
				write_done(iop);
			} else {
				if (oios != nios) {
					auto o1s  = oios;
					auto o1ns = nios - o1s;
					auto o1b  = opattern;

					assert(o1s + o1ns == sector);
					auto iop = std::make_shared<IO>(o1s, o1ns, opattern, 0);
					write_done(iop);
				}

				auto o2s  = nioe + 1;
				auto d    = o2s - old_r.start_sector();
				auto o2ns = old_r.nsectors - d;
				auto o2b  = (*io)->buffer + (d * 512);
				write_done(o2s, o2ns, o2b);
			}
		} else {
			auto d = r.end_sector() - old_r.sector + 1;
			assert(d != 0);
			auto ons = old_r.nsectors - d;
			auto oss = old_r.sector + d;
			assert(r.sector + r.nsectors == oss);
			auto ob  = (*io)->buffer + (d * 512);
			write_done(oss, ons, ob);
		}
	} while (1);
}

int disk::submit_writes(uint64_t nwrites) {
	struct iocb cbs[nwrites];

	uint64_t s;
	uint64_t ns;
	size_t   size;
	for (auto i = 0; i < nwrites; i++) {
		iogen.next_io(&s, &ns)
		assert(ns >= 1);

		string p;
		pattern_create(s, ns, p);

		auto iop = new IO(s, ns, p, 0);
		auto cbp = &cbs[i];
		io_prep_pwrite(cbp, fd, iop->get_buffer(), iop->size(), iop->offset());
		io_set_eventfd(cbp, ioevfdv);
		cbp->data = static_cast<void *> iop;
	}

	return io_submit(this->io_context(), nwrites, &cbs);
}

ssize_t io_result(struct io_event *ep) {
	return (((ssize_t) ep->res2 << 32) | (ssize_t) ep->res);
}

void on_io_complete(evutil_socket_t fd, short what, void *arg) {
	assert(arg != NULL);

	auto d = static_cast<disk *> arg;
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
	unique_ptr<struct io_event[]> eventsup(eventsp);

	/* process IO completion */
	auto fu = std::async(std::launch::deferred, 
			[](disk *d, unique_ptr<struct io_event[]> eventsup, eventfd_t n) {
		struct io_event *eventsp = *ueventsp;		
		for (auto ep = eventsp; ep < eventsp + n; ep++) {
			auto iop = static_cast<IO *> ep->data;
			assert(iop->size() == io_result(ep));

			auto iosp = shared_ptr<IO> iop;
			d->write_done(iosp);
		}
	}, d, std::move(eventsup), nevents);

	/* submit new set of IOs */
	auto rc = d->submit_writes(nevents);
	if (rc < 0) {
		event_base_loopbreak(d->get_event_base());
	}

	fu.wait();
}

int disk::verify() {
	this->eb = event_base_new();
	if (ths->eb == NULL) {
		std::cerr << "libevent initialization failed.\n";
		return -1;
	}

	this->ioevfd = eventfd(0, EFD_NONBLOCK);
	if (ioevfd < 0) {
		std::cerr << "eventfd: %s\n" << strerror(errno);
		return -1;
	}

	this->ioev = event_new(eb, ioevfd, EV_READ, on_io_complete, this);
	if (ioev == NULL) {
		std::cerr << "event_new failed.\n";
		return -1;
	}
	assert(ioev != NULL);

	auto rc = io_setup(io_depth * 2, &this->context);
	if (rc < 0) {
		std::cerr << "io_setup: %s\n" << strerror(errno);
		return -1;
	}

	event_add(ioev, NULL);
	event_base_dispatch(eb);
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