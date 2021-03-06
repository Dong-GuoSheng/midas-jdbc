package cn.synway.bigdata.midas;

import org.apache.http.HttpEntity;
import org.apache.http.entity.AbstractHttpEntity;
import cn.synway.bigdata.midas.util.MidasLZ4OutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LZ4EntityWrapper extends AbstractHttpEntity {
    private final HttpEntity delegate;
    private final int maxCompressBlockSize;
    public LZ4EntityWrapper(AbstractHttpEntity content, int maxCompressBlockSize) {
        this.delegate = content;
        this.maxCompressBlockSize = maxCompressBlockSize;
    }

    public LZ4EntityWrapper(HttpEntity content, int maxCompressBlockSize) {
        delegate = content;
        this.maxCompressBlockSize = maxCompressBlockSize;
    }


    @Override
    public boolean isRepeatable() {
        return delegate.isRepeatable();
    }

    @Override
    public long getContentLength() {
        return -1;
    }

    @Override
    public InputStream getContent() throws IOException, IllegalStateException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
        MidasLZ4OutputStream stream = new MidasLZ4OutputStream(outputStream, maxCompressBlockSize);
        delegate.writeTo(stream);
        stream.flush();
    }

    @Override
    public boolean isStreaming() {
        return delegate.isStreaming();
    }

}
