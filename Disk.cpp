Disk::Disk(std::string path) : path_(std::move(path)) {

}

Disk::~Disk() {
	Close();
}

int Disk::Open() noexcept {
	log_assert(fd_ == -1);
	fd_ = open(path.c_str(), O_RDWR | O_DIRECT);
	if (fd_ < 0) {
		int rc = errno;
		LOG(ERROR) << "Failed to open " << path << " error " << rc;
		return -rc;
	}
	return 0;
}

void Disk::Close() noexcept {
	if (fd_ < 0) {
		return;
	}
	close(fd_);
	fd_ = -1;
}
