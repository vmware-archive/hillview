package org.hillview.remoting;

import com.google.common.net.HostAndPort;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.hillview.dataset.api.IJson;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Describes the list of hosts that comprise a cluster
 */
public final class ClusterDescription implements IJson {
    private final List<HostAndPort> serverList;

    public ClusterDescription(final List<HostAndPort> serverList) {
        this.serverList = serverList;
    }

    public List<HostAndPort> getServerList() {
        return this.serverList;
    }


    public static class HostAndPortSerializer implements JsonSerializer<HostAndPort> {
        public JsonElement serialize(HostAndPort hostAndPort, Type typeOfSchema,
                                     JsonSerializationContext context) {
            return new JsonPrimitive(hostAndPort.toString());
        }
    }

    public static class HostAndPortDeserializer implements JsonDeserializer<HostAndPort> {
        public HostAndPort deserialize(JsonElement json, Type typeOfT,
                                  JsonDeserializationContext context)
                throws JsonParseException {
            return HostAndPort.fromString(json.getAsString());
        }
    }
}