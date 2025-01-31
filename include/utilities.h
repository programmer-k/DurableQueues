#pragma once

#ifndef UTILITIES_H_
#define UTILITIES_H_

#include <atomic>
#include <stddef.h> //for null
#include <climits>  //for max int
#include <fstream>

#define MAX_THREADS 256

#define CACHE_LINE_SIZE 64
#define CACHE_LINE_ALIGNED __attribute__((aligned (CACHE_LINE_SIZE)))
#define DOUBLE_CACHE_LINE_ALIGNED __attribute__((aligned (2 * CACHE_LINE_SIZE)))

static inline void FLUSH(volatile void *p)
{
    asm volatile ("clwb (%0)" :: "r"(p));
}

static inline void SFENCE()
{
    asm volatile ("sfence" ::: "memory");
}

static inline void __writel(uint32_t val, volatile uint32_t *addr)
{
	volatile uint32_t *target = addr;
	asm volatile("movnti %1,%0"
		     : "=m" (*target)
		     : "r" (val) : "memory");
}

static inline void __writeq(uint64_t val, volatile uint64_t *addr)
{
	volatile uint64_t *target = addr;
	asm volatile("movnti %1,%0"
		     : "=m" (*target)
		     : "r" (val) : "memory");
}

static inline void __writeq(void *val, volatile void *addr)
{
	volatile uint64_t *target = (volatile uint64_t *) addr;
	uint64_t value = (uint64_t) val;
	asm volatile("movnti %1,%0"
		     : "=m" (*target)
		     : "r" (value) : "memory");
}

#endif /* UTILITIES_H_ */
