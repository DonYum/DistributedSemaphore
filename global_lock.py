from redlock import RedLock, RedLockError
import time
import redis


class DistributedSemaphore():
    """
    key: 锁名
    expire: 锁idle过期后reset
    concurrent_num: 并发数，可以用来控制loading
    """
    def __init__(self, key, concurrent_num=1, acquire_num=1, expire=None):
        self.key = f'redis_lock:{key}'
        self.concurrent_num = concurrent_num
        self.acquire_num = acquire_num
        self.expire = expire or 3600*1          # 1个小时
        self._redlock_name = f'redlock_global_lock_{key}'
        # self.redis_url = _REDIS_URL
        # self.conn = redis.Redis.from_url(self.redis_url)
        pool = redis.ConnectionPool(host='10.60.242.105', port=16379, db=1)   # , decode_responses=True
        self.conn = redis.Redis(connection_pool=pool)

    def _reset_lock(self):
        self.conn.delete(self.key)

    def _acquire_lock(self):
        with RedLock(self._redlock_name, connection_details=[self.conn], retry_times=30) as _lock:
            res = self.conn.get(self.key)
            if res:
                res = int(res)
                if res + self.acquire_num > self.concurrent_num:     # locked
                    return False
            self.conn.incr(self.key, self.acquire_num)
            self.conn.expire(self.key, self.expire)         # 防止死锁
        return True

    def acquire(self):
        for i in range(self.expire+30):
            if self._acquire_lock():
                return True
            time.sleep(1)
        return False

    def _release(self, acquire_num):
        with RedLock(self._redlock_name, connection_details=[self.conn], retry_times=30) as _lock:
            res = self.conn.decr(self.key, acquire_num)
            if res <= 0:
                self._reset_lock()
        return True

    def release(self):
        return self._release(self.acquire_num)

    def __enter__(self):
        for i in range(20):
            res = self.acquire()
            if res:
                break
            else:                   # 如果获取不到锁，就逐步释放直到能够拿到
                acquire_num = self.acquire_num // 3 or 1
                logger.warn(f'[GlobalLock][{i}]: Acquire Lock FAIL! Try to release {acquire_num}.')
                self._release(acquire_num)

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
