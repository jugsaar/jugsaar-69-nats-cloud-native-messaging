package demo.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public final class Serde {

    public static final ObjectMapper OM;

    static {
        OM = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).findAndRegisterModules();
    }

    public static <T> byte[] jsonBytes(T data) throws Exception {
        return OM.writeValueAsBytes(data);
    }

    public static <T> String json(T data) throws Exception {
        return OM.writeValueAsString(data);
    }

    public static <T> T fromJsonBytes(byte[] data, Class<T> type) throws Exception {
        return OM.readValue(data, type);
    }
}
