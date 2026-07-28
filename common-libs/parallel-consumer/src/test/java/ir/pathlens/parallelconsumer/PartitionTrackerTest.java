package ir.pathlens.parallelconsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.junit.jupiter.api.Test;

class PartitionTrackerTest {

    @Test
    void testCommitsCorrectOffsetAfterAcknowledgement() {
        PartitionTracker partitionTracker = new PartitionTracker();
        partitionTracker.register(100);
        partitionTracker.register(101);
        Optional<Long> offset;
        offset = partitionTracker.complete(103);
        assertTrue(offset.isEmpty());
        offset = partitionTracker.complete(100);
        assertEquals(100, offset.get());
        offset = partitionTracker.complete(102);
        assertTrue(offset.isEmpty());
        offset = partitionTracker.complete(101);
        assertEquals(103, offset.get());
    }
}
