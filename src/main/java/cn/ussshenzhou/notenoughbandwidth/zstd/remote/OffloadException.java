package cn.ussshenzhou.notenoughbandwidth.zstd.remote;

/**
 * Thrown when the remote zstd offload backend fails: native library loading, connection to the offload server,
 * or a native call returning a negative error code.
 *
 * @author USS_Shenzhou
 */
public class OffloadException extends RuntimeException {

    /**
     * The negative error code returned by the native library, or 0 when the failure is not a native error code.
     */
    private final int errorCode;

    public OffloadException(String message) {
        this(message, 0);
    }

    public OffloadException(String message, int errorCode) {
        super(message + " (error code: " + errorCode + ")");
        this.errorCode = errorCode;
    }

    public OffloadException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = 0;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
