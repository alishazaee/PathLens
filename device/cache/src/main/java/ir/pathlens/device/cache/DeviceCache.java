package ir.pathlens.device.cache;

import ir.pathlens.common.model.Page;
import ir.pathlens.device.client.ApiCallException;
import ir.pathlens.device.client.DeviceClient;
import ir.pathlens.device.model.DeviceFilter;
import ir.pathlens.device.model.DeviceResponseDto;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory cache of devices, kept in sync with the device REST API.
 */
public class DeviceCache implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(DeviceCache.class);
    private static final int DEFAULT_PAGE_SIZE = Integer.MAX_VALUE;

    private final DeviceClient client;
    private final ExecutorService syncExecutor = Executors.newSingleThreadExecutor();

    private final int minInitialDelayInMills;
    private final int maxInitialDelayInMils;
    private final int syncIntervalMillis;

    private final AtomicLong currentRevision = new AtomicLong(0);
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Map<String, DeviceResponseDto> devicesMap = new ConcurrentHashMap<>();

    public DeviceCache(DeviceClient deviceClient, int minInitialDelayInMills, int maxInitialDelayInMils,
                       int syncIntervalMillis) {
        this.client = deviceClient;
        this.minInitialDelayInMills = minInitialDelayInMills;
        this.maxInitialDelayInMils = maxInitialDelayInMils;
        this.syncIntervalMillis = syncIntervalMillis;
    }

    /**
     * Starts the periodic background sync thread.
     */
    public void submitCacheSyncBackgroundTask() {
        if (running.compareAndSet(false, true)) {
            syncExecutor.submit(this::runSyncLoop);
        } else {
            logger.warn("Cache sync thread already running.");
        }
    }

    public Optional<DeviceResponseDto> findByDeviceSerialNumber(String serialNumber) {
        return Optional.ofNullable(devicesMap.get(serialNumber));
    }

    public int getTotalDevices() {
        return devicesMap.size();
    }

    public long getRevisionNumber() {
        return currentRevision.get();
    }

    /**
     * Explicit one-off sync with server. It is recommended to use this to make sure that the hashmap is not empty
     */
    public synchronized void sync() throws ApiCallException {
        int initialDelay = ThreadLocalRandom.current().nextInt(minInitialDelayInMills, maxInitialDelayInMils);
        try {
            TimeUnit.MILLISECONDS.sleep(initialDelay);
        } catch (InterruptedException e) {
            logger.warn("Interrupted during initial delay: {}", e.getMessage());
            Thread.currentThread().interrupt();
            return;
        }

        Long serverRevision = client.getRevisionNumber();
        if (serverRevision > currentRevision.get()) {
            logger.info("Updating device cache from revision {} → {}", currentRevision.get(), serverRevision);

            Page<DeviceResponseDto> pagination = Page.of(List.of(), 0, DEFAULT_PAGE_SIZE, 0);
            DeviceFilter filter = new DeviceFilter(true, null, null, null, null, null);
            Page<DeviceResponseDto> response = client.getDevices(filter, pagination);

            Map<String, DeviceResponseDto> updatedMap = new ConcurrentHashMap<>();
            for (DeviceResponseDto dto : response.content()) {
                updatedMap.put(dto.serialNumber(), dto);
            }

            devicesMap = updatedMap;
            currentRevision.set(serverRevision);

            logger.info("Device cache updated successfully. Total devices: {}", devicesMap.size());
        } else {
            logger.debug("Cache already up-to-date (revision {}).", currentRevision.get());
        }
    }

    private void runSyncLoop() {
        while (running.get()) {
            try {
                sync();
                TimeUnit.MILLISECONDS.sleep(syncIntervalMillis);
            } catch (ApiCallException e) {
                logger.error("API call error during cache sync: {}", e.getMessage(), e);
            } catch (InterruptedException e) {
                logger.info("Sync thread interrupted, stopping.");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Unexpected error in sync loop", e);
            }
        }
    }

    @Override
    public void close() {
        logger.info("Shutting down DeviceCache...");
        running.set(false);
        syncExecutor.shutdownNow();
        try {
            if (!syncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Sync executor did not terminate cleanly.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        logger.info("DeviceCache closed.");
        client.close();
    }
}
