package ir.pathlens.device.rest.repository;

import static ir.pathlens.device.rest.db.Tables.LOCATIONS;

import ir.pathlens.device.model.LocationCreateDto;
import ir.pathlens.device.rest.db.tables.records.LocationsRecord;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

/**
 * jOOQ based data access for device locations.
 */
@Repository
@RequiredArgsConstructor
public class LocationRepository {

    private final DSLContext dsl;

    public Optional<LocationsRecord> findById(String siteId) {
        return dsl.selectFrom(LOCATIONS).where(LOCATIONS.SITE_ID.eq(siteId)).fetchOptional();
    }

    public boolean existsById(String siteId) {
        return dsl.fetchExists(dsl.selectOne().from(LOCATIONS).where(LOCATIONS.SITE_ID.eq(siteId)));
    }

    public Map<String, LocationsRecord> findAllByIds(Set<String> siteIds) {
        if (siteIds.isEmpty()) {
            return Map.of();
        }
        return dsl.selectFrom(LOCATIONS).where(LOCATIONS.SITE_ID.in(siteIds)).fetchMap(LOCATIONS.SITE_ID);
    }

    public List<LocationsRecord> findAll(Pageable pageable) {
        return dsl.selectFrom(LOCATIONS)
                .orderBy(LOCATIONS.SITE_ID)
                .limit(pageable.getPageSize())
                .offset(pageable.getOffset())
                .fetch();
    }

    public long count() {
        return dsl.fetchCount(LOCATIONS);
    }

    public LocationsRecord insertIfAbsent(LocationCreateDto request) {
        return dsl.insertInto(LOCATIONS, LOCATIONS.SITE_ID, LOCATIONS.COUNTRY, LOCATIONS.CITY,
                        LOCATIONS.LATITUDE, LOCATIONS.LONGITUDE)
                .values(request.site(), request.country(), request.city(),
                        request.latitude() == null ? null : request.latitude().doubleValue(),
                        request.longitude() == null ? null : request.longitude().doubleValue())
                .onConflict(LOCATIONS.SITE_ID)
                .doNothing()
                .returning(LOCATIONS.fields())
                .fetchOne();
    }

    public void deleteById(String siteId) {
        dsl.deleteFrom(LOCATIONS).where(LOCATIONS.SITE_ID.eq(siteId)).execute();
    }

    public void deleteAll() {
        dsl.deleteFrom(LOCATIONS).execute();
    }
}
