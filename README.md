# DurableQueues
This is the implementation for the paper *Durable Queues: The Second Amendment*, Gal Sela and Erez Petrank, SPAA 2021. See <https://doi.org/10.1145/3409964.3461791>, <https://arxiv.org/abs/2105.08706>.

Build
-----
1. Run `make -C ./include all` for building ssmem.
2. Run `export VMMALLOC_POOL_SIZE=<size>; export VMMALLOC_POOL_DIR="<path>"` (see <https://pmem.io/pmdk/manpages/linux/master/libvmmalloc/libvmmalloc.7.html> for further details regarding libvmmalloc).
3. Run `g++ <your_main> -std=c++11 -L./include -lssmem -latomic -I./include -I./queues -o <executable>` to compile your main file that uses the queues. 
	In your file, before including the queues header files, you should define per-thread allocators, e.g. like so: 
	``` markdown
	__thread ssmem_allocator_t *alloc; 
	__thread ssmem_allocator_t *volatileAlloc;
	```
	They should be initialized in your code for each thread like so:
	``` markdown
	alloc = (ssmem_allocator_t *)malloc(sizeof(ssmem_allocator_t));
    ssmem_alloc_init(alloc, SSMEM_DEFAULT_MEM_SIZE, <thread_id>);
    volatileAlloc = (ssmem_allocator_t *)malloc(sizeof(ssmem_allocator_t));
    ssmem_alloc_init(volatileAlloc, SSMEM_DEFAULT_MEM_SIZE, <thread_id>);
	```

Run
----- 
Run the output file using `LD_PRELOAD=libvmmalloc.so.1 <executable>` (see <https://pmem.io/pmdk/manpages/linux/master/libvmmalloc/libvmmalloc.7.html> for further details regarding libvmmalloc).

*****
A note regarding the memory management: To fully use this code in a crash-recovery scenario, a persistent lock-free memory manager should be utilized. This is an orthogonal open problem which we do not address here. The solution we use is not fully persistent: we use the lock-free ssmem and the underlying libvmmalloc. Therefore, the current recovery code of all our queues is incomplete and was not tested. When a persistent lock-free memory manager will be available in the future, the queues' recovery should be accordingly adjusted.
    
