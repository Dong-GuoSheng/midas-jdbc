package cn.synway.bigdata.midas.util;

import java.io.IOException;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public interface MidasStreamCallback {
    void writeTo(MidasRowBinaryStream stream) throws IOException;
}
