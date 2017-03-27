#ifndef __IO_GEN_H__
#define __IO_GEN_H__

#include <vector>
#include <utility>
#include <algorithm>

#include <cstdint>
#include <cassert>

#include "zipf.h"

using std::vector;
using std::pair;

class block_stats {
public:
	uint64_t nios;
	uint32_t nsectors;
	uint8_t  percent;

	block_stats(uint8_t nsectors, uint8_t percent) {
		this->nsectors = nsectors;
		this->percent  = percent;
		this->nios     = 0;
	}

	void dump(void) {
		std::cout << "# Sectors: " << nsectors << " Percentage " <<
			+percent << " IOs " << nios << std::endl;
	}
};

class io_generator {
private:
	const uint64_t MAX_IO_SIZE  = 1ull << 20;
	const uint64_t SECTOR_SHIFT = 9;
	const uint64_t MAX_SECTORS  = MAX_IO_SIZE >> SECTOR_SHIFT;
private:
	uint64_t sector;   /* start sector */
	uint64_t nsectors; /* number of sectors */
	uint64_t seed;
	uniform  size_rand;
	zipf     sector_rand;

	uint64_t total_ios;
	vector<block_stats> bstat;

public:
	io_generator(uint64_t sector, uint64_t nsectors,
			vector<pair<uint32_t, uint8_t>> &sizes) :
		size_rand(1, 1, MAX_SECTORS),
		sector_rand(0.9, nsectors - MAX_SECTORS, 1)
	{
		uint32_t seed = 1;

		nsectors         -= MAX_SECTORS;
		this->sector      = sector;
		this->nsectors    = nsectors;
		this->seed        = seed;
		this->total_ios   = 0;

		for (auto &it : sizes) {
			auto s = it.first;
			auto p = it.second;

			block_stats b(s, p);
			bstat.push_back(b);
		}

		std::sort(bstat.begin(), bstat.end(),
			[] (const block_stats &a, const block_stats &b) {
				return a.percent > b.percent;
			});
	}

	void next_io(uint64_t *sectorp, uint64_t *nsectorsp) {
		total_ios++;

		uint32_t ns = 0;
		for (auto &it : bstat) {
			auto n = it.nios;
			auto p = it.percent;

			if ((100 * n / total_ios) < p) {
				ns = it.nsectors;
				it.nios++;
				break;
			}
		}

		if (ns == 0) {
			ns = size_rand.next();
			assert(ns >= 1 && ns <= MAX_SECTORS);

			for (auto &it : bstat) {
				if (it.nsectors == ns) {
					it.nios++;
					break;
				}
			}
		}

		auto s = sector_rand.next();
		assert(s >= 0 && s <= this->nsectors);
		s += this->sector;
		assert(s < this->sector + this->nsectors);

		*nsectorsp = ns;
		*sectorp   = s;
	}

	void dump_stats(void) {
		for (auto &it : bstat) {
			it.dump();
		}

		std::cout << "Total IOs " << total_ios << std::endl;
	}
};

#endif
