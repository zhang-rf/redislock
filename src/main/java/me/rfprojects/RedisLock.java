package me.rfprojects;

import redis.clients.jedis.JedisClusterScriptingCommands;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.ScriptingCommands;
import redis.clients.jedis.ShardedJedis;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by zhangrongfan on 2017/10/18.
 */
public class RedisLock implements Lock {

    public static final int DEFAULT_EXPIRE_SECONDS = 30;
    private static final String LOCK_PREFIX = "__lock:";

    private static final String id = UUID.randomUUID().toString();

    private final JedisCommands commands;
    private final String key;
    private final String value;
    private final int expireSeconds;

    private Lock localLock = new ReentrantLock();

    private long minSleepMillis = 50;
    private long maxSleepMillis = 150;

    public RedisLock(JedisCommands commands, String name, int expireSeconds) {
        if (commands == null) {
            throw new IllegalArgumentException();
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException();
        }
        if (expireSeconds <= 0) {
            throw new IllegalArgumentException();
        }

        this.commands = commands;
        this.key = LOCK_PREFIX + digestTextUsingMd5(name);
        this.value = digestTextUsingMd5(key + id);
        this.expireSeconds = expireSeconds;
    }

    public void setSleepMillis(long approximateSleepMillis) {
        if (approximateSleepMillis <= 0) {
            throw new IllegalArgumentException();
        }
        this.minSleepMillis = approximateSleepMillis >> 1;
        this.maxSleepMillis = approximateSleepMillis + this.minSleepMillis;
    }

    @Override
    public void lock() {
        boolean locked = false;
        do {
            try {
                locked = tryLock(0, null);
            } catch (InterruptedException ignored) {
            }
        } while (!locked);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        tryLock(0, null);
    }

    @Override
    public boolean tryLock() {
        boolean locked = false;
        try {
            return locked = localLock.tryLock() && setLock();
        } finally {
            if (!locked) {
                localLock.unlock();
            }
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        if (unit == null) {
            localLock.lockInterruptibly();
        } else {
            long t0 = System.nanoTime();
            if (!localLock.tryLock(time, unit)) {
                return false;
            }
            long t1 = System.nanoTime();
            time -= unit.convert(t1 - t0, TimeUnit.NANOSECONDS);
        }

        if (setLock()) {
            return true;
        }

        boolean locked = false;
        boolean interrupted = false;
        try {
            while ((unit == null || time > 0) && !(interrupted = Thread.interrupted()) &&
                    !(locked = Objects.equals("OK", commands.set(key, value, "NX", "EX", expireSeconds)))) {
                long sleepMillis = ThreadLocalRandom.current().nextLong(minSleepMillis, maxSleepMillis);
                if (unit == null) {
                    Thread.sleep(sleepMillis);
                } else {
                    long sleepTime = Math.min(time,
                            unit.convert(sleepMillis, TimeUnit.MILLISECONDS));
                    unit.sleep(sleepTime);
                    time -= sleepTime;
                }
            }

            if (interrupted) {
                throw new InterruptedException();
            }
            return locked;
        } finally {
            if (!locked) {
                localLock.unlock();
            }
        }
    }

    @Override
    public void unlock() {
        if (localLock.tryLock()) {
            try {
                String script = "if (redis.call('get', KEYS[1]) == ARGV[1]) then " +
                        "redis.call('del', KEYS[1]) end";
                eval(script, 1, key, value);
            } finally {
                localLock.unlock();
            }
        }
    }

    @Override
    public Condition newCondition() {
        return localLock.newCondition();
    }

    private boolean setLock() {
        String script = "if (redis.call('get', KEYS[1]) == ARGV[1]) then " +
                "redis.call('expire', KEYS[1], ARGV[2]) return 'OK' " +
                "else return redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX') end";
        return Objects.equals("OK", eval(script, 1, key, value, String.valueOf(expireSeconds)));
    }

    private Object eval(String script, int keyCount, String... params) {
        if (commands instanceof ScriptingCommands) {
            return ((ScriptingCommands) commands).eval(script, keyCount, params);
        } else if (commands instanceof JedisClusterScriptingCommands) {
            return ((JedisClusterScriptingCommands) commands).eval(script, keyCount, params);
        } else if (commands instanceof ShardedJedis) {
            return ((ShardedJedis) commands).getShard(key).eval(script, keyCount, params);
        }
        throw new UnsupportedOperationException();
    }

    private static String digestTextUsingMd5(String text) {
        try {
            return new BigInteger(1,
                    MessageDigest.getInstance("MD5").digest(text.getBytes())).toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
