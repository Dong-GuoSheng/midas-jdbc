package cn.synway.bigdata.midas.response;

import cn.synway.bigdata.midas.Jackson;
import cn.synway.bigdata.midas.ResponseFactory;

import java.io.IOException;
import java.io.InputStream;

public class MidasResponseFactory implements ResponseFactory<MidasResponse> {
    @Override
    public MidasResponse create(InputStream response) throws IOException {
        return Jackson.getObjectMapper().readValue(response, MidasResponse.class);
    }
}
