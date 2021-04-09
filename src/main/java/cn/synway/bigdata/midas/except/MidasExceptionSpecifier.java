package cn.synway.bigdata.midas.except;

import com.google.common.base.Strings;
import org.apache.http.conn.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.SocketTimeoutException;


/**
 * Specify Midas exception to MidasException and fill it with a vendor code.
 */

public final class MidasExceptionSpecifier {

    private static final Logger log = LoggerFactory.getLogger(MidasExceptionSpecifier.class);

    private MidasExceptionSpecifier() {
    }

    public static MidasException specify(Throwable cause, String host, int port) {
        return specify(cause != null ? cause.getMessage() : null, cause, host, port);
    }

    public static MidasException specify(String MidasMessage, String host, int port) {
        return specify(MidasMessage, null, host, port);
    }

    public static MidasException specify(String MidasMessage) {
        return specify(MidasMessage, "unknown", -1);
    }

    /**
     * Here we expect the Midas error message to be of the following format:
     * "Code: 10, e.displayText() = DB::Exception: ...".
     */
    private static MidasException specify(String MidasMessage, Throwable cause, String host, int port) {
        if (Strings.isNullOrEmpty(MidasMessage) && cause != null) {
            return getException(cause, host, port);
        }

        try {
            int code;
            if (MidasMessage.startsWith("Poco::Exception. Code: 1000, ")) {
                code = 1000;
            } else {
                // Code: 175, e.displayText() = DB::Exception:
                code = getErrorCode(MidasMessage);
            }
            // ошибку в изначальном виде все-таки укажем
            Throwable messageHolder = cause != null ? cause : new Throwable(MidasMessage);
            if (code == -1) {
                return getException(messageHolder, host, port);
            }

            return new MidasException(code, messageHolder, host, port);
        } catch (Exception e) {
            log.error("Unsupported Midas error format, please fix MidasExceptionSpecifier, message: {}, error: {}", MidasMessage, e.getMessage());
            return new MidasUnknownException(MidasMessage, cause, host, port);
        }
    }

    private static int getErrorCode(String errorMessage) {
        int startIndex = errorMessage.indexOf(' ');
        int endIndex = startIndex == -1 ? -1 : errorMessage.indexOf(',', startIndex);

        if (startIndex == -1 || endIndex == -1) {
            return -1;
        }

        try {
        	return Integer.parseInt(errorMessage.substring(startIndex + 1, endIndex));
        } catch(NumberFormatException e) {
        	return -1;
        }
    }

    private static MidasException getException(Throwable cause, String host, int port) {
        if (cause instanceof SocketTimeoutException)
        // if we've got SocketTimeoutException, we'll say that the query is not good. This is not the same as SOCKET_TIMEOUT of Midas
        // but it actually could be a failing Midas
        {
            return new MidasException(MidasErrorCode.TIMEOUT_EXCEEDED.code, cause, host, port);
        } else if (cause instanceof ConnectTimeoutException || cause instanceof ConnectException)
        // couldn't connect to Midas during connectTimeout
        {
            return new MidasException(MidasErrorCode.NETWORK_ERROR.code, cause, host, port);
        } else {
            return new MidasUnknownException(cause, host, port);
        }
    }

}
