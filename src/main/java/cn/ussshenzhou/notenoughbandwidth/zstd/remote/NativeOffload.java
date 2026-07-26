package cn.ussshenzhou.notenoughbandwidth.zstd.remote;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/**
 * Java 25 FFM (java.lang.foreign) binding of the native zstd offload client library (libneb_zstd_client).
 * The library speaks the following C ABI:
 * <pre>
 * long neb_open(const char* addr, int workers, int level, int windowLog, int maxPayload) -> 0 = failure
 * int  neb_compress(long handle, unsigned int connId, const void* src, int srcLen, void* dst, int dstCap) -> >=0 length / &lt;0 error
 * int  neb_compress_oneshot(same signature) -> stateless complete frame
 * int  neb_decompress(long handle, unsigned int connId, const void* src, int srcLen, void* dst, int rawSize) -> 0 ok / &lt;0
 * int  neb_reset_conn(long handle, unsigned int connId) -> 0 / &lt;0
 * void neb_close(long handle)
 * </pre>
 * All ByteBuffers passed to the typed wrapper methods must be direct buffers.
 *
 * @author USS_Shenzhou
 */
public class NativeOffload {

    private final MethodHandle nebOpen;
    private final MethodHandle nebCompress;
    private final MethodHandle nebCompressOneshot;
    private final MethodHandle nebDecompress;
    private final MethodHandle nebResetConn;
    private final MethodHandle nebClose;

    /**
     * The native handle returned by neb_open, 0 = not opened / already closed.
     */
    private long handle = 0;

    private NativeOffload(MethodHandle nebOpen, MethodHandle nebCompress, MethodHandle nebCompressOneshot,
                          MethodHandle nebDecompress, MethodHandle nebResetConn, MethodHandle nebClose) {
        this.nebOpen = nebOpen;
        this.nebCompress = nebCompress;
        this.nebCompressOneshot = nebCompressOneshot;
        this.nebDecompress = nebDecompress;
        this.nebResetConn = nebResetConn;
        this.nebClose = nebClose;
    }

    /**
     * Loads the native library from an absolute path and links every symbol of the C ABI.
     *
     * @throws OffloadException if the library cannot be loaded or a symbol is missing.
     */
    public static NativeOffload load(String absolutePath) {
        try {
            System.load(absolutePath);
        } catch (UnsatisfiedLinkError | SecurityException e) {
            throw new OffloadException("Failed to load native library " + absolutePath, e);
        }
        var lookup = SymbolLookup.loaderLookup();
        var linker = Linker.nativeLinker();
        return new NativeOffload(
                link(linker, lookup, "neb_open", FunctionDescriptor.of(ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_INT)),
                link(linker, lookup, "neb_compress", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)),
                link(linker, lookup, "neb_compress_oneshot", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)),
                link(linker, lookup, "neb_decompress", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT)),
                link(linker, lookup, "neb_reset_conn", FunctionDescriptor.of(ValueLayout.JAVA_INT,
                        ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT)),
                link(linker, lookup, "neb_close", FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG))
        );
    }

    private static MethodHandle link(Linker linker, SymbolLookup lookup, String name, FunctionDescriptor descriptor) {
        var symbol = lookup.find(name)
                .orElseThrow(() -> new OffloadException("Missing native symbol: " + name));
        return linker.downcallHandle(symbol, descriptor);
    }

    /**
     * Connects to the offload server. Must be called exactly once before any other operation.
     *
     * @throws OffloadException if already opened, or neb_open failed.
     */
    public void open(String address, int workers, int level, int windowLog, int maxPayload) {
        if (handle != 0) {
            throw new OffloadException("Native offload handle is already opened.");
        }
        long opened;
        try (var arena = Arena.ofConfined()) {
            var cAddress = arena.allocateFrom(address);
            opened = (long) nebOpen.invokeExact(cAddress, workers, level, windowLog, maxPayload);
        } catch (Throwable t) {
            throw new OffloadException("Failed to call neb_open.", t);
        }
        if (opened == 0) {
            throw new OffloadException("neb_open failed to connect to " + address);
        }
        handle = opened;
    }

    /**
     * Streaming compress: continues the per-connection zstd context and flushes per message.
     *
     * @return compressed size, i.e. how many bytes were written into {@code dst}.
     * @throws OffloadException on negative native return.
     */
    public int compress(int connId, ByteBuffer src, ByteBuffer dst) {
        requireDirect(src, "src");
        requireDirect(dst, "dst");
        int result;
        try {
            result = (int) nebCompress.invokeExact(handle, connId,
                    MemorySegment.ofBuffer(src), src.remaining(), MemorySegment.ofBuffer(dst), dst.remaining());
        } catch (Throwable t) {
            throw new OffloadException("Failed to call neb_compress.", t);
        }
        if (result < 0) {
            throw new OffloadException("neb_compress failed.", result);
        }
        return result;
    }

    /**
     * Stateless compress: produces a complete frame without touching the per-connection zstd context.
     *
     * @return compressed size, i.e. how many bytes were written into {@code dst}.
     * @throws OffloadException on negative native return.
     */
    public int compressOneshot(int connId, ByteBuffer src, ByteBuffer dst) {
        requireDirect(src, "src");
        requireDirect(dst, "dst");
        int result;
        try {
            result = (int) nebCompressOneshot.invokeExact(handle, connId,
                    MemorySegment.ofBuffer(src), src.remaining(), MemorySegment.ofBuffer(dst), dst.remaining());
        } catch (Throwable t) {
            throw new OffloadException("Failed to call neb_compress_oneshot.", t);
        }
        if (result < 0) {
            throw new OffloadException("neb_compress_oneshot failed.", result);
        }
        return result;
    }

    /**
     * Decompresses exactly {@code rawSize} bytes into {@code dst}.
     *
     * @throws OffloadException on negative native return.
     */
    public void decompress(int connId, ByteBuffer src, ByteBuffer dst, int rawSize) {
        requireDirect(src, "src");
        requireDirect(dst, "dst");
        int result;
        try {
            result = (int) nebDecompress.invokeExact(handle, connId,
                    MemorySegment.ofBuffer(src), src.remaining(), MemorySegment.ofBuffer(dst), rawSize);
        } catch (Throwable t) {
            throw new OffloadException("Failed to call neb_decompress.", t);
        }
        if (result < 0) {
            throw new OffloadException("neb_decompress failed.", result);
        }
    }

    /**
     * Resets the per-connection zstd contexts on the offload server.
     *
     * @throws OffloadException on negative native return.
     */
    public void resetConn(int connId) {
        int result;
        try {
            result = (int) nebResetConn.invokeExact(handle, connId);
        } catch (Throwable t) {
            throw new OffloadException("Failed to call neb_reset_conn.", t);
        }
        if (result < 0) {
            throw new OffloadException("neb_reset_conn failed.", result);
        }
    }

    /**
     * Closes the native handle. Safe to call more than once.
     */
    public void close() {
        if (handle == 0) {
            return;
        }
        try {
            nebClose.invokeExact(handle);
        } catch (Throwable t) {
            throw new OffloadException("Failed to call neb_close.", t);
        } finally {
            handle = 0;
        }
    }

    private static void requireDirect(ByteBuffer buffer, String name) {
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException(name + " must be a direct buffer.");
        }
    }
}
