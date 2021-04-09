package cn.synway.bigdata.midas.except;

import java.sql.SQLException;

public class MidasException extends SQLException {

    public MidasException(int code, Throwable cause, String host, int port) {
        super("Midas exception, code: " + code + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }

    public MidasException(int code, String message, Throwable cause, String host, int port) {
        super("Midas exception, message: " + message + ", host: " + host + ", port: " + port + "; "
                + (cause == null ? "" : cause.getMessage()), null, code, cause);
    }
}
