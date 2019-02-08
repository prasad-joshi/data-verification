#pragma once

class LibAio {
public:
	class AIO {
	public:
		AIO(LibAio* aiop);
		int PrepareRead(int fd, void* buf, size_t count, uint64_t offset);
		int PrepareWrite(int fd, void* buf, size_t count, uint64_t offset);
		static int Result(struct io_event* eventp);
	private:
		LibAio aiop_{};
		bool is_read_{false};
		struct iocb iocb;
		folly::Promise<ssize_t> promise_;
	};

public:
	LibAio(folly::EventBase* basep, uint16_t capacity);
	~LibAio();
	void Init();
	int EventFD() const noexcept;
	folly::Future<ssize_t> ScheduleIO(AIO* iop);

private:
	int SubmitInternal() noexcept;
	void HandleIOCompletions(const int to_reap) const;
	void ReadEventFD();

private:
	class EventFDHandler : public folly::EventHandler {
	public:
		EventFDHandler(LibAio* aiop, folly::EventBase* basep, int fd) :
				EventHandler(basep, fd), aiop_(aiop), {
		}

		void handlerReady(uint16_t events) noexcept {
			if (events & EventHandler::READ) {
				aiop->ReadEventFD();
			}
		}
	private:
		LibAio* aiop_{};
	};

	class SchedulePending : public folly::EventBase::LoopCallback {
	public:
		SchedulePending(LibAio* aiop, folly::EventBase* basep) :
			aiop_(aiop), basep_(basep) {
		}
		void runLoopCallback() noexcept override {
			basep_->runBeforeLoop(this);
			auto rc = aiop_->Submit();
			(void) rc;
		}
	private:
		LibAio* aiop_{};
		folly::EventBase* basep_{};
	};


	friend class EventFDHandler;
	friend class SchedulePending;
	static constexpr int16_t kMaxIOCBToSubmit = 128;
private:
	folly::EventBase* basep_{};
	const int16_t capacity_{};
	io_context_t context_;
	int event_fd_{-1};
	std::unique_ptr<EventFDHandler> event_handler_;
	std::unique_ptr<SchedulePending> sched_pending_;

	struct {
		std::mutex mutex_;
		int16_t nfilled_{0};
		constexpr int16_t niocbs_{kMaxIOCBToSubmit};
		struct iocb* iocbpp_[kMaxIOCBToSubmit];
		struct io_event events[kMaxIOCBToSubmit];
	} io_;
};
