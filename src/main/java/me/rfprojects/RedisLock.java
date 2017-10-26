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

    private final JedisCommands commands;
    private final String key;
    private final int expireSeconds;
    private final String id = UUID.randomUUID().toString();

    private Lock localLock = new ReentrantLock();

    public RedisLock(JedisCommands commands, String name, int expireSeconds) {
        try {
            this.commands = commands;
            this.key = LOCK_PREFIX + new BigInteger(1,
                    MessageDigest.getInstance("MD5").digest(name.getBytes())).toString(16);
            this.expireSeconds = expireSeconds;
        } catch (NoSuchAlgorithmException e) {
            throw new ExceptionInInitializerError(e);
        }
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
                    !(locked = Objects.equals("OK", commands.set(key, id, "NX", "EX", expireSeconds)))) {
                long sleepMillis = ThreadLocalRandom.current().nextLong(100, 500);
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
                eval(commands, script, 1, key, id);
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
        return Objects.equals("OK", eval(commands, script, 1, key, id, String.valueOf(expireSeconds)));
    }

    private Object eval(JedisCommands commands, String script, int keyCount, String... params) {
        if (commands instanceof ScriptingCommands) {
            return ((ScriptingCommands) commands).eval(script, keyCount, params);
        } else if (commands instanceof JedisClusterScriptingCommands) {
            return ((JedisClusterScriptingCommands) commands).eval(script, keyCount, params);
        } else if (commands instanceof ShardedJedis) {
            return ((ShardedJedis) commands).getShard(key).eval(script, keyCount, params);
        }
        throw new UnsupportedOperationException();
    }
}
