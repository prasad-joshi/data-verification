using namespace folly;

AIO::AIO(LibAio* aiop) : aiop_(aiop) {
}

int AIO::PrepareRead(int fd, void* buf, size_t count, uint64_t offset) {
	io_prep_pread(&iocb_, fd, buf, count, offset);
	io_set_eventfd(&iocb_, aiop->EventFD());
	iocb.data = reinterpret_cast<void*>(this);
	is_read_ = true;
}

int AIO::PrepareWrite(int fd, void* buf, size_t count, uint64_t offset) {
	io_prep_pwrite(&iocb_, fd, buf, count, offset);
	io_set_eventfd(&iocb_, aiop->EventFD());
	iocb.data = reinterpret_cast<void*>(this);
	is_read_ = false;
}

LibAio(folly::EventBase* basep, uint16_t capacity) : basep_(basep) {
	capacity_ = std::min(capacity, kMaxIOCBToSubmit);
	std::memset(&context_, 0, sizeof(context_));
	auto rc = io_setup(capacity_, &context_);
	log_assert(rc == 0);
}

LibAio::~LibAio() {
	base_->runInEventBaseThreadAndWait([this] () mutable {
		this->event_handler_ = nullptr;
		this->sched_pending_ = nullptr;
	});

	io_destroy(context_);
	if (event_fd_ >= 0) {
		close(event_fd_);
	}
	event_fd_ = -1;
}

int LibAio::Init() {
	event_fd_ = ::eventfd(0, EFD_NONBLOCK);
	if (event_fd_ < 0) {
		int rc = errno;
		LOG(ERROR) << "creating eventfd failed with error " << rc;
		return -rc;
	}

	event_handler_ = std::make_unique<EventFDHandler>(this, basep_, event_fd_);
	if (not event_handler_) {
		LOG(ERROR) << "memory allocation failed";
		return -ENOMEM;
	}

	event_handler_->registerHandler(EventHandler::READ | EventHandler::PERSIST);

	sched_pending_ = std::make_unique<SchedulePending>(this, basep_);
	if (not sched_pending_) {
		LOG(ERROR) << "memory allocation failed";
		return -ENOMEM;
	}
	basep_->runBeforeLoop(sched_pending_.get());
}

int LibAio::SubmitInternal() noexcept {
	if (io_.nfilled_ <= 0) {
		return 0;
	}
	auto rc = io_submit(context_, io_.nfilled_, &io_.iocbpp_);
	if (rc < 0) {
		rc = errno;
		LOG(ERROR) << "io_submit faild with error " << rc;
		return -rc;
	}
	io_.nfilled_ = 0;
	return 0;
}

int LibAio::Submit() {
	std::lock_guard<std::mutex> io_.mutex_;
	return SubmitInternal();
}

folly::Future<ssize_t> LibAio::ScheduleIO(AIO* iop) {
	std::lock_guard<std::mutex> io_.mutex_;
	if (io_.nfilled_ >= io_.niocbs_) {
		int rc = SubmitInternal();
		if (rc < 0) {
			return rc;
		}
	}
	log_assert(io_.nfilled_ < io_.niocbs_);
	iocbpp_[io_.nfilled_++] = &iop->iocb;
	return iop->promise_.getFuture();
}

void LibAio::HandleIOCompletions(const int to_reap) const {
	IOResult = [] (struct io_event* ep) {
		return ((ssize_t)(((uint64_t)ep->res2 << 32) | ep->res));
	};

	for (auto ep = io_.events_; ep < io_.events_ + to_reap; ++ep) {
		auto iop = reinterpret_cast<AIO*>(ep->data);
		ssize_t io_size = IOResult(ep);
		iop->promise_.setValue(io_size);
	}
}

void LibAio::ReadEventFD() {
	eventfd_t nevents;
	while (1) {
		nevents = 0;
		auto rc = ::eventfd_read(event_fd_, &nevents);
		if (rc < 0 or nevents == 0) {
			if (rc < 0 and errno != -EAGAIN) {
				LOG(ERROR) << "eventfd_read failed " << errno;
			}
			break;
		}

		while (nevents > 0) {
			const auto to_reap = std::min(nevents,
				sizeof(io_.events_) / sizeof(io_.events_[0]));

			rc = io_getevents(context_, to_reap, to_reap, io_.events_, nullptr);
			if (rc != to_reap) {
				rc = errno;
				LOG(ERROR) << "failed to get events " << errno;
				return -rc;
			}

			HandleIOCompletions(to_reap);
			nevents -= to_reap;
		}
	}
}
