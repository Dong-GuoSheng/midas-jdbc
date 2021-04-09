package cn.synway.bigdata.midas;

import cn.synway.bigdata.midas.settings.MidasQueryParam;

import java.util.*;

@SuppressWarnings("unchecked")
class ConfigurableApi<T> {

    protected final MidasStatementImpl statement;
    private Map<MidasQueryParam, String> additionalDBParams = new HashMap<MidasQueryParam, String>();
    private Map<String, String> additionalRequestParams = new HashMap<String, String>();

    ConfigurableApi(MidasStatementImpl statement) {
        this.statement = statement;
    }

    Map<String, String> getRequestParams() {
        return additionalRequestParams;
    }

    Map<MidasQueryParam, String> getAdditionalDBParams() {
        return additionalDBParams;
    }

    public T addDbParam(MidasQueryParam param, String value) {
        additionalDBParams.put(param, value);
        return (T) this;
    }

    public T removeDbParam(MidasQueryParam param) {
        additionalDBParams.remove(param);
        return (T) this;
    }

    public T withDbParams(Map<MidasQueryParam, String> dbParams) {
        this.additionalDBParams = new HashMap<MidasQueryParam, String>();
        if (null != dbParams) {
            additionalDBParams.putAll(dbParams);
        }
        return (T) this;
    }

    public T options(Map<String, String> params) {
        additionalRequestParams = new HashMap<String, String>();
        if (null != params) {
            additionalRequestParams.putAll(params);
        }
        return (T) this;
    }

    public T option(String key, String value) {
        additionalRequestParams.put(key, value);
        return (T) this;
    }

    public T removeOption(String key) {
        additionalRequestParams.remove(key);
        return (T) this;
    }

}
