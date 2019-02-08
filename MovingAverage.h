#pragma once

#include <algorithm>

template <typename T, int N>
class MovingAverage {
public:
	void Add(const T& sample) {
		if (nsamples_ < N) {
			total_ += sample;
			samples_[nsamples_++ % N] = sample;
		} else {
			T& old = samples_[nsamples_++ % N];
			total_ += (old - sample);
			old = sample;
		}
	}

	T Average() const noexcept {
		return total_ / std::min(N, nsamples_);
	}

private:
	T samples_[N];
	T total_{0};
	uint64_t nsamples_{0};
};
