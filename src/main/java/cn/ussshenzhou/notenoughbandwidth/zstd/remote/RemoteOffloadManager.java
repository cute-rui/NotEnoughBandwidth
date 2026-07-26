package cn.ussshenzhou.notenoughbandwidth.zstd.remote;

import cn.ussshenzhou.notenoughbandwidth.NotEnoughBandwidthConfig;
import com.mojang.logging.LogUtils;

/**
 * Lazy singleton holding the optional remote zstd offload backend. When the backend is disabled in config,
 * fails to initialize, or has been marked broken after a native error, {@link #available()} returns false
 * and callers should fall back to the local zstd-jni implementation.
 *
 * @author USS_Shenzhou
 */
public class RemoteOffloadManager {

    private static volatile RemoteOffloadManager instance;

    private final NativeOffload offload;
    private volatile boolean broken = false;

    private RemoteOffloadManager(NativeOffload offload) {
        this.offload = offload;
    }

    public static synchronized RemoteOffloadManager get() {
        if (instance == null) {
            instance = init();
        }
        return instance;
    }

    private static RemoteOffloadManager init() {
        var config = NotEnoughBandwidthConfig.get();
        if (!config.remoteOffloadEnabled) {
            return new RemoteOffloadManager(null);
        }
        try {
            var offload = NativeOffload.load(config.remoteOffloadLibrary);
            offload.open(config.remoteOffloadAddress, config.remoteOffloadWorkers, 3, config.getContextLevel(), config.getMaxPacketSize());
            LogUtils.getLogger().info("Remote zstd offload enabled: connected to {} with {} workers.", config.remoteOffloadAddress, config.remoteOffloadWorkers);
            return new RemoteOffloadManager(offload);
        } catch (Exception e) {
            LogUtils.getLogger().error("Failed to initialize remote zstd offload, falling back to local compression.", e);
            return new RemoteOffloadManager(null);
        }
    }

    public boolean available() {
        return offload != null && !broken;
    }

    /**
     * Circuit breaker: once called, {@link #available()} returns false for the rest of this JVM's lifetime,
     * so that subsequent connections fall back to local compression.
     */
    public void markBroken() {
        if (!broken) {
            broken = true;
            LogUtils.getLogger().error("Remote zstd offload marked as broken, falling back to local compression.");
        }
    }

    /**
     * @return the native offload handle, or null when the backend is not available.
     */
    public NativeOffload instance() {
        return offload;
    }
}
