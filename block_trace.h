#ifndef __BLOCK_TRACE_LOG_H__
#define __BLOCK_TRACE_LOG_H__

#include <fstream>
#include <ctime>

using std::string;
using std::time_t;
using std::fstream;

struct block_trace {
	time_t   timestamp_;
	uint64_t sector_;
	uint16_t nsectors_;
	uint8_t  read_:1;
	uint8_t  pad_[5];

	block_trace() {
	}
	block_trace(time_t &t, uint64_t sector, uint16_t nsectors, bool read) : 
		timestamp_(t), sector_(sector), nsectors_(nsectors), read_(read) {

	}
};

class TraceLog {
private:
	string   logPrefix_;
	uint32_t current_;
	string   curLogFile_;
	int      curLogFD_;

	fstream  curFS_;
private:
#if 0
	void open();
	void close();
	void compress();
#endif

public:
	TraceLog(string logPrefix);
	~TraceLog();
	void addTraceLog(uint64_t sector, uint16_t nsectors, bool read);
	// std::vector<block_trace> searchTraceLog(uint64_t sector, uint16_t nsectors);
	void dumpTraceLog(uint64_t sector, uint16_t nsectors);
};

#endif
