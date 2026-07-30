package ir.pathlens.parallelconsumer;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Tracks per-partition offsets and determines the highest contiguous offset that can be committed.
 */
public class PartitionTracker {
    private final Set<Long> completed = new HashSet<>();
    private long nextCommit = -1;

    /**
     * Registers an offset as seen/in-progress.
     */
    public void register(long offset) {
        if (nextCommit == -1) {
            nextCommit = offset;
        }
    }

    /**
     * Marks an offset as completed. Returns the highest contiguous committed offset if a new range was completed, or
     * empty otherwise.
     */
    public Optional<Long> complete(long offset) {
        completed.add(offset);

        long highestCommit = -1;
        while (completed.remove(nextCommit)) {
            highestCommit = nextCommit;
            nextCommit++;
        }

        if (highestCommit != -1) {
            return Optional.of(highestCommit);
        }

        return Optional.empty();
    }

    /**
     * Returns the next offset to commit (the highest contiguous completed offset + 1), -1 if nothing has been
     * registered.
     */
    public long getNextCommitOffset() {
        return nextCommit;
    }
}
