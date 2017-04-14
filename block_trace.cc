#include <iostream>
#include <ios>
#include <vector>
#include <fstream>
#include <chrono>
#include <ctime>
#include <cassert>
#include <string>

#include "block_trace.h"

using std::string;
using std::vector;
using std::fstream;
using std::cout;
using std::endl;
using std::ios;
using std::time_t;

TraceLog::TraceLog(string logPrefix) : logPrefix_(logPrefix),
			curFS_(logPrefix, ios::out | ios::binary | ios::app) {


}

TraceLog::~TraceLog() {
	if (curFS_.is_open()) {
		curFS_.flush();
		curFS_.sync();
		curFS_.close();
	}
}

void TraceLog::addTraceLog(uint64_t sector, uint16_t nsectors, bool read) {
	auto ct = std::chrono::system_clock::now();
	auto st = std::chrono::system_clock::to_time_t(ct);

	block_trace trace{st, sector, nsectors, read};

	curFS_.write((char *) &trace, sizeof(trace));
}

void TraceLog::dumpTraceLog(uint64_t sector, uint16_t nsectors) {
	curFS_.flush();
	curFS_.sync();

	fstream is(logPrefix_, ios::in | ios::binary);
	assert(is && is.is_open());
	is.seekg(0, is.beg);

	auto r1_low      = sector;
	auto r1_high     = sector + nsectors - 1;
	auto f           = true;
	auto nreads      = 0ul;
	auto nwrites     = 0ul;
	auto nbytesRead  = 0ull;
	auto nbytesWrote = 0ull;
	while (!is.eof()) {
		block_trace t;
		is.read((char *)&t, sizeof(t));

		auto r2_low  = t.sector_;
		auto r2_high = t.sector_ + t.nsectors_ - 1;

		if (r2_low < r1_high && r2_high > r1_low) {
			/* Intercepting IO */
			if (f == false) {
				if (nreads) {
					cout << nbytesRead << " bytes Read in " << nreads << " Reads " << endl;
				}
				if (nwrites) {
					cout << nbytesWrote << " bytes Wrote in " << nwrites << " Writes " << endl;	
				}
				nreads      = 0;
				nbytesRead  = 0;
				nwrites     = 0;
				nbytesWrote = 0;
			}

			auto ioc = t.read_ ? 'R' : 'W';
			string time(ctime(&t.timestamp_));
			time.pop_back();
			cout << time << " ===> " << ioc << " " << t.sector_ << " " << t.nsectors_ << endl;

			f = false;
			continue;
		}

		if (f == true) {
			string time(ctime(&t.timestamp_));
			time.pop_back();
			cout << "====== Start Time " << time << " =====\n";
			f = false;
		}

		if (t.read_) {
			nreads++;
			nbytesRead += (t.nsectors_ << 9);
		} else {
			nwrites++;
			nbytesWrote += (t.nsectors_ << 9);
		}
	}
}