package com.telcobright.statewalk.persistence.redis;

import com.telcobright.statewalk.persistence.MachineSnapshot;
import com.telcobright.statewalk.persistence.PersistenceProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Redis implementation of {@link PersistenceProvider} — the recommended
 * provider for high-TPS realtime session stores (a hash write per transition
 * beats a JDBC upsert by an order of magnitude, and restore scans read the
 * index set instead of a table scan).
 *
 * <h2>Layout</h2>
 * <pre>
 *   sw:{registry}:{machineId}       HASH   one snapshot (fields below)
 *   sw:idx:{registry}               SET    machineIds with a live snapshot
 *   sw:dead:{registry}:{machineId}  HASH   quarantined snapshot + reason
 *   sw:deadidx:{registry}           SET    quarantined machineIds
 * </pre>
 * Save = MULTI(HSET + SADD); delete = MULTI(DEL + SREM) — the index can never
 * disagree with the data. {@code loadAllForRegistry} = SMEMBERS + pipelined
 * HGETALL (one round trip for the index, one pipelined burst for the rows) —
 * no N+1 point reads.
 *
 * <p>Optional dependency: add {@code redis.clients:jedis} to use this class;
 * the rest of statewalk never touches it.
 *
 * <p>Optionally pass a TTL: snapshots then expire on their own if a registry
 * dies and never comes back — pick a TTL comfortably above your longest
 * session (0 = keep forever, the default).
 */
public class RedisPersistenceProvider implements PersistenceProvider {

    private static final Logger LOG = LoggerFactory.getLogger(RedisPersistenceProvider.class);

    private static final String F_STATE      = "state";
    private static final String F_CTX_CLASS  = "ctx_class";
    private static final String F_CTX_B64    = "ctx_b64";
    private static final String F_SAVED_AT   = "saved_at";
    private static final String F_T_TARGET   = "t_target";
    private static final String F_T_DEADLINE = "t_deadline";
    private static final String F_G_DEADLINE = "g_deadline";
    private static final String F_DEAD_REASON = "dead_reason";
    private static final String F_DEAD_AT     = "dead_at";

    private final JedisPool pool;
    private final int ttlSeconds;

    public RedisPersistenceProvider(JedisPool pool) {
        this(pool, 0);
    }

    /** @param ttlSeconds snapshot TTL; 0 = no expiry. */
    public RedisPersistenceProvider(JedisPool pool, int ttlSeconds) {
        this.pool = pool;
        this.ttlSeconds = Math.max(0, ttlSeconds);
        // Fail fast on a dead server — a registry must not come up believing
        // it has persistence when it does not.
        try (Jedis j = pool.getResource()) {
            j.ping();
        }
    }

    private static String key(String registry, String machineId)     { return "sw:" + registry + ":" + machineId; }
    private static String idxKey(String registry)                    { return "sw:idx:" + registry; }
    private static String deadKey(String registry, String machineId) { return "sw:dead:" + registry + ":" + machineId; }
    private static String deadIdxKey(String registry)                { return "sw:deadidx:" + registry; }

    @Override
    public void save(MachineSnapshot s) {
        Map<String, String> h = new HashMap<>();
        h.put(F_STATE, s.currentState());
        if (s.contextClassName() != null)  h.put(F_CTX_CLASS, s.contextClassName());
        if (s.contextJsonBase64() != null) h.put(F_CTX_B64, s.contextJsonBase64());
        h.put(F_SAVED_AT, Long.toString(s.savedAtMs()));
        if (s.timeoutTargetState() != null) h.put(F_T_TARGET, s.timeoutTargetState());
        h.put(F_T_DEADLINE, Long.toString(s.timeoutDeadlineMs()));
        h.put(F_G_DEADLINE, Long.toString(s.globalDeadlineMs()));

        String k = key(s.registryName(), s.machineId());
        try (Jedis j = pool.getResource()) {
            Transaction t = j.multi();
            t.del(k);                                  // full replace — stale optional fields must not survive
            t.hset(k, h);
            if (ttlSeconds > 0) t.expire(k, ttlSeconds);
            t.sadd(idxKey(s.registryName()), s.machineId());
            t.exec();
        } catch (RuntimeException e) {
            throw new RuntimeException("redis save failed for "
                + s.machineId() + "/" + s.registryName() + ": " + e.getMessage(), e);
        }
    }

    @Override
    public Optional<MachineSnapshot> load(String machineId, String registryName) {
        try (Jedis j = pool.getResource()) {
            Map<String, String> h = j.hgetAll(key(registryName, machineId));
            if (h == null || h.isEmpty()) return Optional.empty();
            return Optional.of(fromHash(machineId, registryName, h));
        } catch (RuntimeException e) {
            throw new RuntimeException("redis load failed for "
                + machineId + "/" + registryName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<MachineSnapshot> loadAll(String machineId) {
        // The compound key embeds the registry; without knowing every registry
        // name a full-keyspace scan would be needed. Statewalk's core only
        // calls the (machineId, registryName) point read and the per-registry
        // scans, so this returns the point read shape: not supported here.
        throw new UnsupportedOperationException(
            "loadAll(machineId) across registries is not supported by the Redis provider; "
            + "use load(machineId, registryName) or loadAllForRegistry(registryName)");
    }

    @Override
    public void delete(String machineId, String registryName) {
        try (Jedis j = pool.getResource()) {
            Transaction t = j.multi();
            t.del(key(registryName, machineId));
            t.srem(idxKey(registryName), machineId);
            t.exec();
        } catch (RuntimeException e) {
            throw new RuntimeException("redis delete failed for "
                + machineId + "/" + registryName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void quarantine(String machineId, String registryName, String reason) {
        try (Jedis j = pool.getResource()) {
            Map<String, String> h = j.hgetAll(key(registryName, machineId));
            if (h == null || h.isEmpty()) return;      // idempotent
            h.put(F_DEAD_REASON, reason != null ? reason : "unknown");
            h.put(F_DEAD_AT, Long.toString(System.currentTimeMillis()));
            Transaction t = j.multi();
            String dk = deadKey(registryName, machineId);
            t.del(dk);
            t.hset(dk, h);
            t.sadd(deadIdxKey(registryName), machineId);
            t.del(key(registryName, machineId));
            t.srem(idxKey(registryName), machineId);
            t.exec();
            LOG.warn("quarantined snapshot {}/{} → {} ({})", machineId, registryName, dk, reason);
        } catch (RuntimeException e) {
            throw new RuntimeException("redis quarantine failed for "
                + machineId + "/" + registryName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public List<MachineSnapshot> loadMatured(String registryName, long nowMs) {
        List<MachineSnapshot> out = new ArrayList<>();
        for (MachineSnapshot s : loadAllForRegistry(registryName)) {
            if (s.timeoutFiredBy(nowMs)) out.add(s);
        }
        return out;
    }

    @Override
    public List<MachineSnapshot> loadAllForRegistry(String registryName) {
        try (Jedis j = pool.getResource()) {
            Set<String> ids = j.smembers(idxKey(registryName));
            if (ids == null || ids.isEmpty()) return List.of();
            List<String> ordered = new ArrayList<>(ids);
            Pipeline p = j.pipelined();
            List<Response<Map<String, String>>> resps = new ArrayList<>(ordered.size());
            for (String id : ordered) {
                resps.add(p.hgetAll(key(registryName, id)));
            }
            p.sync();
            List<MachineSnapshot> out = new ArrayList<>(ordered.size());
            List<String> stale = new ArrayList<>();
            for (int i = 0; i < ordered.size(); i++) {
                Map<String, String> h = resps.get(i).get();
                if (h == null || h.isEmpty()) { stale.add(ordered.get(i)); continue; }  // TTL-expired row
                out.add(fromHash(ordered.get(i), registryName, h));
            }
            if (!stale.isEmpty()) {
                j.srem(idxKey(registryName), stale.toArray(new String[0]));
            }
            return out;
        } catch (RuntimeException e) {
            throw new RuntimeException("redis loadAllForRegistry failed for " + registryName
                + ": " + e.getMessage(), e);
        }
    }

    private static MachineSnapshot fromHash(String machineId, String registryName, Map<String, String> h) {
        return new MachineSnapshot(
            machineId,
            registryName,
            h.get(F_STATE),
            h.get(F_CTX_CLASS),
            h.get(F_CTX_B64),
            parseLong(h.get(F_SAVED_AT)),
            h.get(F_T_TARGET),
            parseLong(h.get(F_T_DEADLINE)),
            parseLong(h.get(F_G_DEADLINE)));
    }

    private static long parseLong(String v) {
        if (v == null || v.isEmpty()) return 0L;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return 0L; }
    }

    /** Test helper: live snapshot count for a registry (index cardinality). */
    public long size(String registryName) {
        try (Jedis j = pool.getResource()) {
            return j.scard(idxKey(registryName));
        }
    }

    /** Test helper: quarantined snapshot count for a registry. */
    public long deadSize(String registryName) {
        try (Jedis j = pool.getResource()) {
            return j.scard(deadIdxKey(registryName));
        }
    }
}
