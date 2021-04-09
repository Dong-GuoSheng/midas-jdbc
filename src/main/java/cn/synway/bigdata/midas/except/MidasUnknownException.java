package cn.synway.bigdata.midas.except;

public class MidasUnknownException extends MidasException {

    public MidasUnknownException(Throwable cause, String host, int port) {
        super(MidasErrorCode.UNKNOWN_EXCEPTION.code, cause, host, port);
    }


    public MidasUnknownException(String message, Throwable cause, String host, int port) {
        super(MidasErrorCode.UNKNOWN_EXCEPTION.code, message, cause, host, port);
    }

    public MidasUnknownException(Integer code, Throwable cause, String host, int port) {
        super(code, cause, host, port);
    }

}
