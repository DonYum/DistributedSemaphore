from redlock import RedLock, RedLockError
import time
import redis


class DistributedSemaphore():
    """
    key: 锁名
    expire: 锁idle过期后reset
    concurrent_num: 并发数，可以用来控制loading
    """
    def __init__(self, key, concurrent_num=1, acquire_num=1, expire=3600*24*10, redis_url='redis://localhost:6379/0'):
        self.key = f'redis_lock_{key}'
        self.concurrent_num = concurrent_num
        self.acquire_num = acquire_num
        self.expire = expire
        self._redlock_name = f'redlock_global_lock_{key}'
        self.redis_url = redis_url
        self.conn = redis.Redis.from_url(self.redis_url)

    def _reset_lock(self):
        self.conn.delete(self.key)

    def _acquire_lock(self):
        with RedLock(self._redlock_name, connection_details=[{'url': self.redis_url}]) as _lock:
            res = self.conn.get(self.key)
            if res:
                res = int(res)
                if res + self.acquire_num > self.concurrent_num:     # locked
                    return False
            self.conn.incr(self.key, self.acquire_num)
            self.conn.expire(self.key, self.expire)
        return True

    def acquire(self):
        if self._acquire_lock():
            return True
        for i in range(self.expire):
            if self._acquire_lock():
                return True
            time.sleep(1)
        return False

    def release(self):
        with RedLock(self._redlock_name, connection_details=[{'url': self.redis_url}]) as _lock:
            res = self.conn.decr(self.key, self.acquire_num)
            if res <= 0:
                self._reset_lock()
            else:
                self.conn.expire(self.key, self.expire)
        return True

    def __enter__(self):
        if not self.acquire():
            raise RedLockError('failed to acquire global lock')

    def __exit__(self, exc_type, exc_value, traceback):
        self.release()
