# DistributedSemaphore

分布式信号量。

Distributed Semaphore using redlock(redis)

用于分布式计算中的资源竞争场景，或资源请求的削峰处理。

底层分布式互斥锁使用的是RedLock，可以保证`DistributedSemaphore`请求、释放的原子性。

## Usage

使用起来很简单，比如：

- 消耗CPU/GPU资源的场景：

    使用calc_with_cpu()做计算消耗30% CPU计算资源，这时候可以这样使用：

    ```python
    lock_tag = 'cpu_loading'
    with GlobalLock(lock_tag, 10, 3) as g_lock:
        calc_with_cpu()
    ```

    GPU资源、内存、网络资源竞争的情况也一样。

- 资源请求的削峰处理：

    为了保护MongoDB的负载稳定在一定loading情况下，需要对请求做削峰处理。
    比如限制read_from_mongo()的并发为5，可以这样使用：

    ```python
    lock_tag = 'read_mongo'
    with GlobalLock(lock_tag, 5, 1) as g_lock:
        read_from_mongo()
    ```
