package org.dkv.client;

import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;
import static org.dkv.client.Utils.checkf;

/**
 * Represents a shard or partition of keyspace owned by
 * a DKV instance. It is identified by a user defined
 * name along with one or more {@link DKVNodeSet} instances
 * identified by the respective {@link DKVOpType}.
 *
 * @see ShardedDKVClient
 */
public class DKVShard {
    private final String name;
    private final Map<DKVOpType, DKVNodeSet> topology;

    public DKVShard(String name, Map<DKVOpType, DKVNodeSet> topology) {
        checkf(name != null && !name.trim().isEmpty(), IllegalArgumentException.class, "shard name must be provided");
        validate(topology);

        this.name = name;
        this.topology = unmodifiableMap(topology);
    }

    private void validate(Map<DKVOpType, DKVNodeSet> topology) {
        checkf(topology != null && !topology.isEmpty(), IllegalArgumentException.class, "topology must be given");
        //noinspection ConstantConditions
        for (Map.Entry<DKVOpType, DKVNodeSet> topEntry : topology.entrySet()) {
            DKVOpType opType = topEntry.getKey();
            checkf(opType != DKVOpType.UNKNOWN, IllegalArgumentException.class, "DKV operation type must be given");
            DKVNodeSet nodes = topEntry.getValue();
            checkf(nodes != null, IllegalArgumentException.class, "DKV nodes must be given");
        }
    }

    public String getName() {
        return name;
    }

    public DKVNodeSet getNodesByOpType(DKVOpType opType) {
        checkf(opType != null && this.topology.containsKey(opType), IllegalArgumentException.class, "valid DKV operation type must be given");
        checkf(this.topology != null, IllegalStateException.class, "topology is not initialized");
        //noinspection ConstantConditions
        return this.topology.get(opType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DKVShard dkvShard = (DKVShard) o;
        return name.equals(dkvShard.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "DKVShard{" +
                "name='" + name + '\'' +
                ", topology=" + topology +
                '}';
    }

    // intended for deserialization
    @SuppressWarnings("unused")
    private DKVShard() {
        this.name = null;
        this.topology = null;
    }
}
