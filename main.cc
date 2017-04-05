#include <iostream>

#include <cassert>
#include <gflags/gflags.h>

#include "io_generator.h"
#include "disk_io.h"

using std::vector;
using std::pair;
using std::string;
using std::cout;
using std::endl;

DEFINE_string(disk, "/dev/null", "Comma seperated list of block devices for IO verification");
DEFINE_int32(iodepth, 32, "Number of concurrent IOs");
DEFINE_int32(percent, 100, "Percent of block device to use for IOs");
DEFINE_string(blocksize, "4096:40,8192:40",	"Typical block sizes for IO.");
DEFINE_string(runtime, "1h", "runtime in (s)seconds/(m)minutes/(h)hours/(d)days");
DEFINE_string(logpath, "/tmp/", "Log directory path");

vector<string> split(const string &str, char delim) {
	std::vector<string> tokens;
	size_t s = 0;
	size_t e = 0;

	while ((e = str.find(delim, s)) != string::npos) {
		if (e != s) {
			tokens.push_back(str.substr(s, e - s));
			s = e + 1;
		}
	}
	if (e != s) {
		tokens.push_back(str.substr(s));
	}
	return tokens;
}

void bytesToHumanReadable(uint64_t nbytes, uint64_t &valp, string &unitp) {
	vector<string> units = {
		"B",
		"KB",
		"MB",
		"GB",
		"TB",
	};
	for (auto i = 0; i < 4; i++) {
		auto t = nbytes;
		nbytes >>= 10;
		if (nbytes == 0) {
			valp  = t;
			unitp = units[i];
			return;
		}
	}
	valp  = nbytes;
	unitp = "TB";
}

int main(int argc, char *argv[]) {
	google::ParseCommandLineFlags(&argc, &argv, true);

	/* check block sizes */
	auto tokens = split(FLAGS_blocksize, ',');
	if (!tokens.size()) {
		throw std::invalid_argument("Block sizes not given.");
	}

	auto tp = 0;
	vector<pair<uint32_t, uint8_t>> sizes;
	for (auto &t : tokens) {
		auto e = t.find(':');
		if (e == string::npos) {
			cout << "Invalid block size " << t << endl;
			continue;
		}
		auto bs = std::stoul(t.substr(0, e));
		auto p  = std::stoul(t.substr(e+1));
		if (!bs || !p || bs % 512 != 0 || p > 100) {
			cout << "Invalid block size " << t << endl;
			continue;
		}

		sizes.push_back(std::make_pair(bs>>9, p));
		tp += p;
	}

	if (!sizes.size()) {
		throw std::invalid_argument("Block sizes not given.");
	} else if (tp > 100) {
		throw std::invalid_argument("Invalid Blocksizes.\n");
	}

	/* check IO Depth */
	if (FLAGS_iodepth <= 0 || FLAGS_iodepth > 512) {
		throw std::invalid_argument("iodepth > 0 and iodepth < 512");
	}

	/* check percentage */
	if (FLAGS_percent <= 0 || FLAGS_percent > 100) {
		throw std::invalid_argument("percent > 0 and percent < 100");
	}

	/* check runtime */
	auto m  = 1ull;
	auto &c = FLAGS_runtime.back();
	switch (c) {
	case 'd':
	case 'D':
		m *= 24;
	case 'h': /* fall through */
	case 'H':
		m *= 60;
	case 'm': /* fall through */
	case 'M':
		m *= 60;
	case 's': /* fall through */
	case 'S':
		m *= 1;
		break;
	default:
		auto d = '9' - c;
		if (d < 0 || d > 9) {
			throw std::invalid_argument("Invalid Runtime");
		}
		m *= 1;
	}

	double runtime;
	try {
		runtime = std::stod(FLAGS_runtime);
		if (errno == ERANGE || runtime == 0.0) {
			throw std::invalid_argument("stoul");
		}
	} catch (std::exception &e) {
		throw std::invalid_argument("Invalid Runtime");
	}

	runtime *= m;

	/* constuct disk object */
	disk d1(FLAGS_disk, FLAGS_percent, sizes, FLAGS_iodepth, (uint64_t)runtime);

	/* print some information */
	cout << "Disk " << FLAGS_disk << endl;
	cout << "Disk size in sectors " << d1.nsectors() << endl;
	cout << "Number of sectors for IOs " << d1.ioNSectors() << endl;
	for (auto &s : sizes) {
		cout << "Block Size = " << (s.first << 9) << " " << (int) s.second << "%\n";
	}
	cout << "IODepth " << FLAGS_iodepth << endl;
	cout << "Runtime " << runtime << " seconds\n";

	d1.verify();

	uint64_t nr, nw, nbr, nbw;
	d1.getStats(&nr, &nw, &nbr, &nbw);
	uint64_t r;
	string ur;
	bytesToHumanReadable(nbr, r, ur);
	uint64_t w;
	string uw;
	bytesToHumanReadable(nbw, w, uw);

	cout << endl;
	cout << "Total IOs " << nr + nw << endl;
	cout << "Read (Verification) IOs " << nr << " Read (Verified) Bytes " << nbr << " (" << r << ur << ")" << endl;
	cout << "Write IOs " << nw << " Wrote Bytes " << nbw << " (" << w << uw << ")" << endl;
	return 0;
}
