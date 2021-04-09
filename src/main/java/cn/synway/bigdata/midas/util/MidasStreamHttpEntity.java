package cn.synway.bigdata.midas.util;

import cn.synway.bigdata.midas.settings.MidasProperties;
import com.google.common.base.Preconditions;
import org.apache.http.entity.AbstractHttpEntity;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.TimeZone;

/**
 * @author Dmitry Andreev <a href="mailto:AndreevDm@yandex-team.ru"></a>
 */
public class MidasStreamHttpEntity extends AbstractHttpEntity {

    private final MidasStreamCallback callback;
    private final TimeZone timeZone;
    private final MidasProperties properties;

    public MidasStreamHttpEntity(MidasStreamCallback callback, TimeZone timeZone, MidasProperties properties) {
        Preconditions.checkNotNull(callback);
        this.timeZone = timeZone;
        this.callback = callback;
        this.properties = properties;
    }

    @Override
    public boolean isRepeatable() {
        return true;
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        return null;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        MidasRowBinaryStream stream = new MidasRowBinaryStream(out, timeZone, properties);
        callback.writeTo(stream);
    }

    @Override
    public boolean isStreaming() {
        return false;
    }
}
