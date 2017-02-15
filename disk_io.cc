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

std::shared_ptr<char> IO::get_buffer(size_t *sizep)
{
	auto sz = sector_to_byte(r.nsectors);

	char *bp = new char[sz];
	if (bp == nullptr) {
		*sizep = 0;
		return nullptr;
	}

	/* create shared pointer to return */
	std::shared_ptr<char> rbp(bp, [] (char *p) { delete[] p; });

	*sizep = sz;

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
}

disk::~disk()
{
	if (this->fd > 0) {
		close(this->fd);
	}
}

void disk::pattern_create(uint64_t sector, uint16_t nsectors, string &pattern)
{
	pattern = "<" + std::to_string(sect) + "," + std::to_string(nsec) + ">";
}

uint32_t disk::write(uint64_t sector, uint16_t nsectors)
{
	string pattern;
	pattern_create(sector, nsectors, pattern);

	auto iop = std::make_shared<IO>(sector, nsectors, pattern, 0);
	auto buf = io.get_buffer(&size);

#if 0
	char *bp = buf.get();
	write(fd, bp, size);
#endif

	write_done(sector, nsectors);
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
