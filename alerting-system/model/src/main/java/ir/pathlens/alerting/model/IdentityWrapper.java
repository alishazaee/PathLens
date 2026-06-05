package ir.pathlens.alerting.model;

import ir.pathlens.proto.CameraLogProto;
import jakarta.persistence.Embeddable;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.validation.constraints.NotNull;
import java.util.Objects;

/**
 * Simple wrapper that holds an {@link IdentityType} and its corresponding identityValue. Provides a unified way to
 * represent identities (e.g., phone or plate number) extracted from a {@link CameraLogProto.Log}.
 */
@Embeddable
public record IdentityWrapper(
        @NotNull(message = "Identity type is required")
        @Enumerated(EnumType.STRING)
        IdentityType identityType,
        @NotNull(message = "Identity identityValue is required")
        String identityValue
) {

    public static IdentityWrapper of(IdentityType type, CameraLogProto.Log log) {
        Objects.requireNonNull(type, "identity type must not be null");
        Objects.requireNonNull(log, "log must not be null");
        if (type == IdentityType.PhoneNumber) {
            return new IdentityWrapper(type, log.getPhoneNumber());
        } else if (type == IdentityType.PlateNumber) {
            return new IdentityWrapper(type, log.getPlateNumber());
        }
        throw new IllegalStateException("unknow identity type");
    }
}
