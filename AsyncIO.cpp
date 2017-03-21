#include <cassert>
#include <cstdlib>

#include <sys/eventfd.h>
#include <libaio.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

#include "AsyncIO.h"

using namespace folly;

enum class IOType {
	READ,
	WRITE,
};

class io {
public:
	uint64_t      offset_;
	size_t        size_;
	int           fd_;
	ManagedBuffer bufp_;
	IOType        type_;
private:
	AsyncIO  *asynciop_;
public:
	io(AsyncIO *asynciop, int fd, ManagedBuffer bufp, size_t size, uint64_t offset, IOType type) : bufp_(std::move(bufp)) {
		offset_   = offset;
		size_     = size;
		fd_       = fd;
		asynciop_ = asynciop;
		type_     = type;
	}
};

AsyncIO::AsyncIO(uint16_t capacity) :
			capacity_(capacity), eventfd_(-1), handlerp_(nullptr), initialized_(false) {
	std::memset(&context_, 0, sizeof(context_));
	auto rc = io_setup(capacity_, &context_);
	assert(rc == 0);

	nsubmitted  = 0;
	ncompleted  = 0;
	nreads      = 0;
	nwrites     = 0;
	nbytesRead  = 0;
	nbytesWrote = 0;
}

AsyncIO::~AsyncIO() {
	if (eventfd_ >= 0) {
		close(eventfd_);
	}
	if (handlerp_) {
		delete(handlerp_);
	}
	io_destroy(context_);
}

void AsyncIO::init(EventBase *basep) {
	eventfd_ = eventfd(0, EFD_NONBLOCK);
	assert(eventfd_ >= 0);
	handlerp_ = new EventFDHandler(this, basep, eventfd_);
	assert(handlerp_);
	handlerp_->registerHandler(EventHandler::READ | EventHandler::PERSIST);
	initialized_ = true;
}

void AsyncIO::registerCallback(IOCompleteCB iocb, NIOSCompleteCB niocb, void *cbdata) {
	iocbp_   = iocb;
	cbdatap_ = cbdata;
	niocbp_   = niocb;
}

ssize_t AsyncIO::ioResult(struct io_event *ep) {
	return ((ssize_t)(((uint64_t)ep->res2 << 32) | ep->res));
}

void AsyncIO::iosCompleted() {
	assert(iocbp_ && eventfd_ >= 0);

	eventfd_t nevents;
	uint16_t  completed = 0;

	while (1) {
		nevents = 0;
		int rc  = eventfd_read(eventfd_, &nevents);
		if (rc < 0 || nevents == 0) {
			if (rc < 0 && errno != EAGAIN) {
				assert(0);
			}
			assert(errno == EAGAIN);
			/* no more events to process */
			break;
		}

		assert(nevents > 0);
		struct io_event events[nevents];
		rc = io_getevents(context_, nevents, nevents, events, NULL);
		assert(rc == nevents);

		for (auto ep = events; ep < events + nevents; ep++) {
			auto *iop   = reinterpret_cast<io*>(ep->data);
			auto result = ioResult(ep);
			bool read   = iop->type_ == IOType::READ;
			if (read) {
				this->nbytesRead  += iop->size_;
			} else {
				this->nbytesWrote += iop->size_;
			}
			iocbp_(cbdatap_, std::move(iop->bufp_), iop->size_, iop->offset_, result, read);
			delete iop;
		}
		completed += nevents;
	}

	this->ncompleted += completed;
	if (niocbp_) {
		niocbp_(cbdatap_, completed);
	}
}

uint64_t AsyncIO::getPending() {
	return this->nsubmitted - this->ncompleted;
}

void AsyncIO::pwritePrepare(struct iocb *iocbp, int fd, ManagedBuffer bufp, size_t size, uint64_t offset) {
	assert(initialized_ && iocbp && bufp && fd >= 0);
	std::memset(iocbp, 0, sizeof(*iocbp));

	char *b = bufp.get();
	io_prep_pwrite(iocbp, fd, b, size, offset);
	io_set_eventfd(iocbp, eventfd_);
	auto iop  = new io(this, fd, std::move(bufp), size, offset, IOType::WRITE);
	iocbp->data = reinterpret_cast<void *>(iop);
	assert(iop);
}

int AsyncIO::pwrite(struct iocb **iocbpp, int nwrites) {
	assert(initialized_ && iocbpp && nwrites);
	this->nwrites    += nwrites;
	this->nsubmitted += nwrites;
	return io_submit(context_, nwrites, iocbpp);
}

void AsyncIO::preadPrepare(struct iocb *iocbp, int fd, ManagedBuffer bufp, size_t size, uint64_t offset) {
	assert(initialized_ && iocbp && bufp && fd >= 0);
	std::memset(iocbp, 0, sizeof(*iocbp));

	char *b = bufp.get();
	io_prep_pread(iocbp, fd, b, size, offset);
	io_set_eventfd(iocbp, eventfd_);
	auto iop  = new io(this, fd, std::move(bufp), size, offset, IOType::READ);
	iocbp->data = reinterpret_cast<void *>(iop);
	assert(iop);
}

int AsyncIO::pread(struct iocb **iocbpp, int nreads) {
	assert(initialized_ && iocbpp && nreads);
	this->nreads     += nreads;
	this->nsubmitted += nreads;
	return io_submit(context_, nreads, iocbpp);
}

#define PAGE_SIZE 4096
#define DEFAULT_ALIGNMENT PAGE_SIZE

ManagedBuffer AsyncIO::getIOBuffer(size_t size) {
	void *bufp{};
	auto rc = posix_memalign(&bufp, DEFAULT_ALIGNMENT, size);
	assert(rc == 0 && bufp);
	return ManagedBuffer(reinterpret_cast<char *>(bufp), free);
}