package cn.ussshenzhou.notenoughbandwidth.zstd;

import java.nio.ByteBuffer;

/**
 * A per-connection zstd compression/decompression backend. Implementations must produce the exact same wire format:
 * magicless frames, flush per message, streaming dictionary when the underlying context is reused.
 *
 * @author USS_Shenzhou
 */
public interface ZstdContext {

    /**
     * Compresses the remaining bytes of {@code raw}.
     *
     * @return a direct buffer, position = 0, limit = compressed size.
     */
    ByteBuffer compress(ByteBuffer raw);

    /**
     * Decompresses the remaining bytes of {@code compressed}, whose decompressed size is exactly {@code originalSize}.
     *
     * @return a direct buffer, position = 0, limit = {@code originalSize}.
     */
    ByteBuffer decompress(ByteBuffer compressed, int originalSize);

    /**
     * Releases any resource held by this context. Best-effort: must not throw checked exceptions.
     */
    void close();
}
