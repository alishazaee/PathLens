package ir.pathlens.device.rest.repository;

import static ir.pathlens.device.rest.db.Tables.DEVICE;
import static ir.pathlens.device.rest.db.Tables.LOCATIONS;

import ir.pathlens.device.model.DeviceCreateRequestDto;
import ir.pathlens.device.model.DeviceFilter;
import ir.pathlens.device.model.DeviceStatus;
import ir.pathlens.device.model.DeviceType;
import ir.pathlens.device.model.DeviceUpdateRequestDto;
import ir.pathlens.device.rest.db.tables.records.DeviceRecord;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.impl.DSL;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

/**
 * jOOQ based data access for devices.
 */
@Repository
@RequiredArgsConstructor
public class DeviceRepository {

    private final DSLContext dsl;

    public Optional<DeviceRecord> findById(int id) {
        return dsl.selectFrom(DEVICE).where(DEVICE.ID.eq(id)).fetchOptional();
    }

    public List<DeviceRecord> findBySiteId(String siteId) {
        return dsl.select(DEVICE.fields())
                .from(DEVICE)
                .where(DEVICE.SITE_ID.equal(siteId))
                .fetchInto(DEVICE);
    }

    public Optional<DeviceRecord> findBySerialNumber(String serialNumber) {
        return dsl.selectFrom(DEVICE).where(DEVICE.SERIAL_NUMBER.eq(serialNumber)).fetchOptional();
    }

    public Optional<LocalDateTime> findMaxUpdatedAt() {
        return dsl.select(DSL.max(DEVICE.UPDATED_AT)).from(DEVICE).fetchOptional().map(Record1::value1);
    }

    public List<DeviceRecord> findAll(DeviceFilter filter, Pageable pageable) {
        return dsl.select(DEVICE.fields())
                .from(DEVICE)
                .join(LOCATIONS).on(DEVICE.SITE_ID.eq(LOCATIONS.SITE_ID))
                .where(toCondition(filter))
                .orderBy(DEVICE.ID)
                .limit(pageable.getPageSize())
                .offset(pageable.getOffset())
                .fetchInto(DEVICE);
    }

    public long count(DeviceFilter filter) {
        return dsl.fetchCount(dsl.select(DEVICE.fields())
                .from(DEVICE)
                .join(LOCATIONS).on(DEVICE.SITE_ID.eq(LOCATIONS.SITE_ID))
                .where(toCondition(filter)));
    }

    public DeviceRecord insert(DeviceCreateRequestDto request) {
        return dsl.insertInto(DEVICE, DEVICE.SERIAL_NUMBER, DEVICE.DEVICE_TYPE, DEVICE.STATUS, DEVICE.SITE_ID)
                .values(request.serialNumber(), toEnumName(request.type()), toEnumName(request.status()),
                        request.siteId())
                .returning(DEVICE.fields())
                .fetchOne();
    }

    public Optional<DeviceRecord> update(int id, DeviceUpdateRequestDto request) {
        return dsl.update(DEVICE)
                .set(DEVICE.DEVICE_TYPE, toEnumName(request.type()))
                .set(DEVICE.STATUS, toEnumName(request.status()))
                .set(DEVICE.SITE_ID, request.siteId())
                .set(DEVICE.UPDATED_AT, LocalDateTime.now())
                .where(DEVICE.ID.eq(id))
                .returning(DEVICE.fields())
                .fetchOptional();
    }

    public void deleteById(int id) {
        dsl.deleteFrom(DEVICE).where(DEVICE.ID.eq(id)).execute();
    }

    public void deleteAll() {
        dsl.deleteFrom(DEVICE).execute();
    }

    private static Condition toCondition(DeviceFilter filter) {
        Condition condition = DSL.noCondition();
        if (filter.justActiveDevices() != null) {
            DeviceStatus status = filter.justActiveDevices() ? DeviceStatus.ACTIVE : DeviceStatus.INACTIVE;
            condition = condition.and(DEVICE.STATUS.eq(status.name()));
        }
        if (filter.serialNumber() != null) {
            condition = condition.and(DEVICE.SERIAL_NUMBER.eq(filter.serialNumber()));
        }
        DeviceType type = filter.type();
        if (type != null) {
            condition = condition.and(DEVICE.DEVICE_TYPE.eq(type.name()));
        }
        if (filter.minLatitude() != null) {
            condition = condition.and(LOCATIONS.LATITUDE.greaterOrEqual(filter.minLatitude().doubleValue()));
        }
        if (filter.maxLatitude() != null) {
            condition = condition.and(LOCATIONS.LATITUDE.lessOrEqual(filter.maxLatitude().doubleValue()));
        }
        if (filter.minLongitude() != null) {
            condition = condition.and(LOCATIONS.LONGITUDE.greaterOrEqual(filter.minLongitude().doubleValue()));
        }
        if (filter.maxLongitude() != null) {
            condition = condition.and(LOCATIONS.LONGITUDE.lessOrEqual(filter.maxLongitude().doubleValue()));
        }
        return condition;
    }

    private static String toEnumName(Enum<?> value) {
        return value == null ? null : value.name();
    }
}
