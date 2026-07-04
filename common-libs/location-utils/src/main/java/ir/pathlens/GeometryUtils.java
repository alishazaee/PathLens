package ir.pathlens;

import java.util.Random;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKTReader;

/**
 * Utility class for geometry operations.
 */
public class GeometryUtils {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final Random random = new Random();

    /**
     * Creates a random rectangular polygon WKT.
     */
    public static String createRandomWkt() {

        double minLon = -180 + random.nextDouble() * 350;
        double minLat = -90 + random.nextDouble() * 170;

        double width = 0.1 + random.nextDouble() * 2.0;
        double height = 0.1 + random.nextDouble() * 2.0;

        double maxLon = minLon + width;
        double maxLat = minLat + height;

        return String.format(
                "POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))",
                minLon, minLat,
                maxLon, minLat,
                maxLon, maxLat,
                minLon, maxLat,
                minLon, minLat
        );
    }

    /**
     * Returns a random point INSIDE the polygon.
     */
    public static LatLon getRandomPointInsideWkt(String wkt) throws Exception {

        Geometry polygon = new WKTReader().read(wkt);
        Envelope envelope = polygon.getEnvelopeInternal();

        while (true) {

            double lon = envelope.getMinX()
                    + random.nextDouble()
                    * (envelope.getMaxX() - envelope.getMinX());

            double lat = envelope.getMinY()
                    + random.nextDouble()
                    * (envelope.getMaxY() - envelope.getMinY());

            Point point = GEOMETRY_FACTORY.createPoint(
                    new Coordinate(lon, lat)
            );

            if (polygon.contains(point)) {
                return new LatLon(lat, lon);
            }
        }
    }

    public static LatLon getRandomPointOutsideWkt(String wkt) throws Exception {

        Geometry polygon = new WKTReader().read(wkt);

        while (true) {

            double lon = -180 + random.nextDouble() * 360;
            double lat = -90 + random.nextDouble() * 180;

            Point point = GEOMETRY_FACTORY.createPoint(
                    new Coordinate(lon, lat)
            );

            if (!polygon.contains(point)) {
                return new LatLon(lat, lon);
            }
        }
    }

    /**
     * Represents a latitude/longitude coordinate pair.
     */
    public record LatLon(double latitude, double longitude) {}
}