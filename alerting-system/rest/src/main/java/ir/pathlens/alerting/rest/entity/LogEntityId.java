package ir.pathlens.alerting.rest.entity;

import java.io.Serializable;
import java.util.UUID;

/**
 * Composite identifier for target log entities.
 */
public record LogEntityId(
        UUID id,
        UUID rule
) implements Serializable {
}
