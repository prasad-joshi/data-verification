#pragma once

#include <string>
#include <memory>
#include <utility>

namespace Log {

enum class Operation {
	kWrite = 1ul << 0,
	kStart = 1ul << 1,
	kCommit = 1ul << 2,
};

class ReadStream {
public:
	ReadStream(std::string dir, size_t file_min_no, size_t file_max_no);
	int Read(uint64_t* sectorp, uint16_t* nsectorsp, Operation* opp, bool* morep);
private:
	int Open(bool* has_morep);
	int OpenNext(bool *has_morep);
	int Read(char* datap, size_t length);
private:
	const std::string dir_;
	struct {
		const size_t min_no_{};
		const size_t max_no_{};
		size_t cur_no_{};
	} file_;
};

class WriteStream;
class IntentLog {
public:
	IntentLog(std::string dir);
	int Open();
	int Write(uint64_t sector, uint16_t nsectors, Operation op) noexcept;
	std::unique_ptr<ReadStream> OpenReadStream();
	int Write(uint64_t sector, uint16_t nsectors, Operation op);
public:
	static std::string FileNoFilePath(const std::string& dir, size_t file_no);
	static size_t FileSize(const std::string& file);
private:
	std::pair<size_t, size_t> FindFileNo() const;
private:
	const std::string dir_;
	struct {
		size_t min_no_{-1};
		size_t max_no_{-1};
	} file_;
	std::unique_ptr<WriteStream> write_stream_;
};
}
