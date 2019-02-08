#pragma once

class Disk {
public:
	Disk(folly::EventBase* basep, std::string path);
	~Disk();
	int Open() noexcept;
	size_t Size() const noexcept;
	folly::Future<int> Write(const char* datap, uint64_t sector,
		uint16_t nsectors);
	folly::Future<int> Read(char* datap, uint64_t sector, uint16_t nsectors);
private:
	const std::string path_{};
	folly::EventBase* basep_{};
	int fd_{-1};

	struct {
		std::mutex mutex_;
		uint64_t nwrites_{0};
		uint64_t wrote_bytes_{0};
		uint64_t write_latency_{0};
		MovingAverage<uint64_t, 128> write_latency_avg_;

		uint64_t nreads_{0};
		uint64_t read_bytes_{0}
		uint64_t read_latency_{0};
		MovingAverage<uint64_t, 128> read_latency_avg_;

		uint64_t ios_progress_{0};
	} stats_;
};
