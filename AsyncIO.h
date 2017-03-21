#ifndef __ASYNCIO_H__
#define __ASYNCIO_H__

#include <libaio.h>
#include <folly/io/async/EventBase.h>
#include <folly/io/async/EventHandler.h>

using namespace folly;
using std::unique_ptr;
using std::shared_ptr;

typedef unique_ptr<char, void(*)(void*)> ManagedBuffer;
typedef std::function<void(void *cbdata, ManagedBuffer bufp, size_t size, uint64_t offset, ssize_t result, bool read)> IOCompleteCB;
typedef std::function<void(void *cbdata, uint16_t nios)> NIOSCompleteCB;

class AsyncIO {
private:
	io_context_t   context_;
	int            eventfd_;
	uint16_t       capacity_;
	bool           initialized_;

	uint64_t       nsubmitted;
	uint64_t       ncompleted;
	uint64_t       nwrites;
	uint64_t       nreads;
	uint64_t       nbytesRead;
	uint64_t       nbytesWrote;
private:
	NIOSCompleteCB niocbp_;
	IOCompleteCB   iocbp_;
	void           *cbdatap_;

private:
	ssize_t ioResult(struct io_event *ep);

public:
	class EventFDHandler : public EventHandler {
	private:
		int       fd_;
		EventBase *basep_;
		AsyncIO   *asynciop_;
	public:
		EventFDHandler(AsyncIO *asynciop, EventBase *basep, int fd) :
				fd_(fd), basep_(basep), asynciop_(asynciop), EventHandler(basep, fd) {
			assert(fd >= 0 && basep);
		}

		void handlerReady(uint16_t events) noexcept {
			assert(events & EventHandler::READ);
			if (events & EventHandler::READ) {
				asynciop_->iosCompleted();
			}
		}
	};

	AsyncIO(uint16_t capcity);
	~AsyncIO();

	void init(EventBase *basep);
	void registerCallback(IOCompleteCB iocb, NIOSCompleteCB niocb, void *cbdata);
	void iosCompleted();
	void pwritePrepare(struct iocb *cbp, int fd, ManagedBuffer bufp, size_t size, uint64_t offset);
	int  pwrite(struct iocb **iocbpp, int nwrites);
	void preadPrepare(struct iocb *cbp, int fd, ManagedBuffer bufp, size_t size, uint64_t offset);
	int  pread(struct iocb **iocbpp, int nwrites);
	ManagedBuffer getIOBuffer(size_t size);
	uint64_t getPending();

private:
	EventFDHandler *handlerp_;
};

#endif