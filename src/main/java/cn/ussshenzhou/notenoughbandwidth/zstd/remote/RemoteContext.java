package cn.ussshenzhou.notenoughbandwidth.zstd.remote;

import cn.ussshenzhou.notenoughbandwidth.zstd.ZstdContext;
import com.github.luben.zstd.Zstd;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ZstdContext} backed by the remote zstd offload server. The wire format is byte-identical to the
 * local zstd-jni implementation: same level, same windowLog, magicless frames, flush per message.
 *
 * @author USS_Shenzhou
 */
public class RemoteContext implements ZstdContext {

    private static final AtomicInteger NEXT_CONN_ID = new AtomicInteger(1);

    private final int connId;
    private final boolean useContext;

    public RemoteContext(boolean useContext) {
        this.connId = NEXT_CONN_ID.getAndIncrement();
        this.useContext = useContext;
    }

    @Override
    public ByteBuffer compress(ByteBuffer raw) {
        try {
            var src = ensureDirect(raw);
            var dst = ByteBuffer.allocateDirect((int) Zstd.compressBound(raw.remaining()));
            int compressedSize;
            if (useContext) {
                compressedSize = RemoteOffloadManager.get().instance().compress(connId, src, dst);
            } else {
                compressedSize = RemoteOffloadManager.get().instance().compressOneshot(connId, src, dst);
            }
            dst.position(0);
            dst.limit(compressedSize);
            return dst;
        } catch (OffloadException e) {
            RemoteOffloadManager.get().markBroken();
            throw e;
        }
    }

    @Override
    public ByteBuffer decompress(ByteBuffer compressed, int originalSize) {
        try {
            var src = ensureDirect(compressed);
            var dst = ByteBuffer.allocateDirect(originalSize);
            RemoteOffloadManager.get().instance().decompress(connId, src, dst, originalSize);
            dst.position(0);
            dst.limit(originalSize);
            return dst;
        } catch (OffloadException e) {
            RemoteOffloadManager.get().markBroken();
            throw e;
        }
    }

    /**
     * The native library requires direct buffers; copy into a direct scratch buffer when given a heap-based one.
     */
    private static ByteBuffer ensureDirect(ByteBuffer buffer) {
        if (buffer.isDirect()) {
            return buffer;
        }
        var direct = ByteBuffer.allocateDirect(buffer.remaining());
        direct.put(buffer);
        direct.flip();
        return direct;
    }

    @Override
    public void close() {
        try {
            var offload = RemoteOffloadManager.get().instance();
            if (offload != null) {
                offload.resetConn(connId);
            }
        } catch (Exception ignored) {
            //best-effort reset; the server also cleans up contexts of gone connections by itself
        }
    }
}
