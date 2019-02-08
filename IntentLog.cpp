#include <experimental/filesystem>
#include <string>
#include <regex>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace Log {
namespace fs = std::filesystem;

constexpr size_t kMinFileNo = 1;
constexpr size_t kLogFileSizeMax = 4ull << 20;

struct Record {
	uint64_t sector;
	uint16_t nsectors;
	uint16_t op;
};

class WriteStream {
public:
	WriteStream(std::string dir) : dir_(std::move(dir)) {
	}

	~WriteStream() {
		Close();
	}

	void Close() noexcept {
		if (fd_ < 0) {
			return;
		}
		fdatasync(fd_);
		fsync(fd_);
		close(fd_);
		write_size_ = 0;
		fd_ = -1;
		return;
	}

	size_t WriteSize() const noexcept {
		return write_size_;
	}

	bool IsOpen() const noexcept {
		return fd_ >= 0;
	}

	int Open(const size_t file_no) {
		Close();
		auto file = IntentLog::FileNoFilePath(dir_, file_no);
		fd_ = ::open(file.c_str(), O_WRONLY | O_SYNC | O_CREAT);
		if (fd_ < 0) {
			int rc = errno
			LOG(ERROR) << "Opening a file failed with error " << rc;
			return -rc;
		}
		return 0;
	}

	int Write(uint64_t sector, uint16_t nsectors, Operation op) noexcept {
		Record record;
		record.sector = sector;
		record.nsectors = nsectors;
		record.op = op;

		auto data = reinterpret_cast<const char*>(&record);
		return Write(data, sizeof(record));
	}

	int Write(const char* data, const size_t length) noexcept {
		size_t rc = std::write(fd_, data, length);
		if (rc < length) {
			rc = errno;
			LOG(ERROR) << "write failed with error " << rc;
			return -rc;
		}
		write_size_ += length;
		return 0;
	}

private:
	const std::string dir_{};
	int fd_{-1};
	size_t write_size_{};
};

ReadStream::ReadStream(std::string dir, size_t min, size_t max) : dir_(std::move(dir)),
		file_(min, max, min) {
}

ReadStream::~ReadStream() {
	if (fd_ >= 0) {
		close(fd_);
	}
}

int ReadStream::Open(bool* has_morep) {
	if (file_.cur_no_ <= kMinFileNo or file_.cur_no_ >= file_.max_no_) {
		*has_morep = false;
		return 0;
	}

	*has_morep = true;
	auto file = FileNoFilePath(dir, file_.cur_no_);
	fd_ = ::open(file.c_str(), O_RDONLY);
	if (fd_ < 0) {
		int rc = errno;
		LOG(ERROR) << "failed to open file " << file << " error" << rc;
		return -rc
	}
	return 0;
}

int ReadStream::OpenNext(bool *has_morep) {
	if (fd_ >= 0) {
		close(fd_);
		fd_ = -1;
	}
	++file_.cur_no_;
	return Open(has_morep);
}

int ReadStream::Read(uint64_t* sectorp, uint16_t* nsectorsp, Operation* opp, bool* morep) {
	if (fd_ < 0) {
		LOG(ERROR) << "Either ReadStream is not opened or has reached EOF";
		*has_morep = false;
		return 0;
	}

	Record record;
	*morep = true;
	auto data = reinterpret_cast<char*>(&record);
	auto rc = Read(data, sizeof(record));
	if (rc < 0) {
		return rc
	} else if (rc == 0) {
		rc = OpenNext(morep);
		if (not *morep) {
			return 0;
		} else if (rc < 0) {
			return rc;
		}
		return Read(sectorp, nsectorsp, opp, morep);
	}
	log_assert(rc == sizeof(record));
	*sectorp = record.sector;
	*nsectorsp = record.nsectors;
	*opp = record.opp;
	return 0;
}

int ReadStream::Read(char* datap, size_t length) {
	return ::read(fd_, datap, length);
}

std::pair<size_t, size_t> IntentLog::FindFileNo() const {
	if (not fs::exists(diri_) or not fs::is_directory(dir_)) {
		std::cerr << "Directory " << dir_
			<< " either not present or not a directory";
		return -EINVAL;
	}

	size_t max_no = std::numeric_limit<size_t>::min();
	size_t min_no = std::numeric_limit<size_t>::max();
	const std::regex re(".*/log-(\\d+)\\.bin$");
	std::smatch match;
	for (const auto& entry : fs::directory_iterator(dir_)) {
		const auto& path = entry.path().string(); 
		if (not std::regex_match(path, match, re) or match.size() != 2) {
			continue;
		}
		size_t n = std::stoul(match[1].str());
		max_no = std::max(max_no, n);
		min_no = std::min(min_no, n);
	}
	if (min_no > max_no) {
		return {kMinFileNo-1, kMinFileNo-1};
	}
	return {min_no, max_no};
}

IntentLog::IntentLog(std::string dir) : dir_(std::move(dir)), file_(FindFileNo()) {
}

int IntentLog::Open() {
	std::lock_guard<std::mutex> lock(mutex_);
	if (write_stream_ and write_stream_->IsOpen()) {
		return 0;
	}

	auto no = std::max(file_.max_no_, kMinFileNo);
	auto size = FileSize(FileNoFilePath(dir_, no));
	if (size >= kLogFileSizeMax) {
		++no;
	}
	write_stream_ = std::make_unique<WriteStream>(dir_);
	auto rc = write_stream_->Open(no);
	if (rc < 0) {
		return rc;
	}
	file_.max_no_ = no;
	return 0;
}

int IntentLog::Write(uint64_t sector, uint16_t nsectors, Operation op) noexcept {
	std::lock_guard<std::mutex> lock(mutex_);
	if (not write_stream_->IsOpen()) {
		LOG(ERROR) << "Cannot write to intent log. WriteStream is not open";
		return -1;
	}

	auto rc = write_stream_->Write(sector, nsectors, op);
	if (rc < 0) {
		return 0;
	}
	if (write_stream_->WriteSize() >= kLogFileSizeMax) {
		rc = write_stream_->Open(file_.max_no_ + 1);
		if (rc < 0) {
			return rc;
		}
		++file_.max_no_;
		log_assert(write_stream_->WriteSize() == 0);
	}
	return 0;
}

std::unique_ptr<ReadStream> IntentLog::OpenReadStream() {
	auto read = std::make_unique<ReadStream>(dir_, file_.min_no_, file_.max_no_);
	return read;
}

std::string IntentLog::FileNoFilePath(const std::string& dir, size_t file_no) {
	std::ostringstream os;
	os << dir << "/log-" << file_no << ".bin";
	return os.str();
}

size_t IntentLog:FileSize(const std::string& file) {
	try {
		if (not fs::is_regular_file(file)) {
			return 0;
		}
		return fs::file_size(file);
	} catch (const std::exeception& e) {
		return 0;
	}
}
}
