#ifndef __ZIP_F_H__
#define __ZIP_F_H__

/*
 * MOST OF THE SOURCE CODE IS COPIED FROM fio SOURCE CODE
 * */

#include <random>
#include <cstdint>
#include <cmath>

class frand {
private:
	uint32_t s1;
	uint32_t s2;
	uint32_t s3;

private:
	uint32_t __seed(uint32_t x, uint32_t m) {
		return (x < m) ? x + m : x;
	}

public:
	static const uint32_t FRAND_MAX = -1U;

	frand(uint32_t seed) {
		auto LCG = [] (uint64_t x, uint32_t seed) {
			return ((x) * 69069 ^ (seed));
		};

		s1 = __seed(LCG((2^31) + (2^17) + (2^7), seed), 1);
		s2 = __seed(LCG(s1, seed), 7);
		s3 = __seed(LCG(s2, seed), 15);
	}

	frand() : frand(1) {}

	unsigned int rand() {
		auto TAUSWORTHE = [] (uint32_t s, int a, int b, uint32_t c, int d) {
			return ((s&c)<<d) ^ (((s <<a) ^ s)>>b);
		};
		s1 = TAUSWORTHE(s1, 13, 19, 4294967294UL, 12);
		s2 = TAUSWORTHE(s2, 2, 25, 4294967288UL, 4);
		s3 = TAUSWORTHE(s3, 3, 11, 4294967280UL, 17);
		return (s1 ^ s2 ^ s3);
	}

};

class zipf {
private:
	const uint64_t MAX_GEN     = 10000000ul;
	const uint64_t GR_PRIME_64 = 0x9e37fffffffc0001ULL;
	frand    rand;
	double   theta;
	uint64_t nitems; /* number of items to choose from */
	double   zetan;  /* precalculated ZetaN, based on nitems */
	double   zeta2;  /* precalculated Zeta2, based on theta */
	uint64_t rand_off;
	uint32_t seed;

private:
	/* aux function to calculate zeta */
	double zetan_calculate(void) {
		auto MIN = [] (uint64_t a, uint64_t b) {
			return (a < b ? a : b);
		};

		double ans = 0.0;

		auto n = MIN(this->nitems, MAX_GEN);
		for (uint64_t i = 1; i <= n; i++)
			ans += pow(1.0 / (double)i, theta);
		return ans;
	}

public:
	zipf(double theta, double nitems, uint32_t seed) : rand(seed) {
		this->theta  = theta;
		this->nitems = nitems;
		this->seed   = seed;
		zetan        = zetan_calculate();
		zeta2        = pow(1.0, theta) + pow(0.5, theta);
		rand_off     = rand.rand();
	}

	uint64_t next() {
		auto hash_u64 = [=] (uint64_t v) {
			return v * GR_PRIME_64;
		};
		double   alpha, eta, rand_uni, rand_z;
		uint64_t n = nitems;
		uint64_t val;

		alpha = 1.0 / (1.0 - theta);
		eta   = (1.0 - pow(2.0 / n, 1.0 - theta)) / (1.0 - zeta2 / zetan);
		rand_uni = (double) rand.rand() / (double) frand::FRAND_MAX;
		rand_z   = rand_uni * zetan;

		if (rand_z < 1.0) {
			val = 1;
		} else if (rand_z < (1.0 + pow(0.5, theta))) {
			val = 2;
		} else {
			val = 1 + (uint64_t)(n * pow(eta*rand_uni - eta + 1.0, alpha));
		}

		return (hash_u64(val - 1) + rand_off) % nitems;
	}

	uint32_t get_seed() {
		return seed;
	}
};


class uniform {
private:
	std::mt19937 eng{std::random_device{}()};
	uint64_t     min;
	uint64_t     max;
	uint32_t     seed;

public:
	uniform() = default;
	uniform(uint32_t seed, uint64_t min = 1, uint64_t max = 100000000ull) :
		eng(seed)
	{
		this->min  = min;
		this->max  = max;
		this->seed = seed;
	}

	uint64_t next() {
		return std::uniform_int_distribution<uint64_t>{min, max}(eng);
	}

	uint64_t get_min() { return min; }
	uint64_t get_max() { return max; }
	uint64_t get_seed() { return seed; }
};
#endif
