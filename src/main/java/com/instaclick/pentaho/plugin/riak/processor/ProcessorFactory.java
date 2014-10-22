package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginMeta;
import java.io.IOException;

public class ProcessorFactory
{
    private RiakClient clientFor(final RiakPluginData data) throws IOException
    {
        return RiakClient.newClient(data.port, data.host.split(","));
    }

    public Processor processorFor(final RiakPlugin step,final RiakPluginData data, final RiakPluginMeta meta) throws IOException
    {
        final RiakClient client = clientFor(data);

        if (RiakPluginData.Mode.GET == data.mode) {
            return new GetProcessor(client, step, data);
        }

        if (RiakPluginData.Mode.PUT == data.mode) {
            return new PutProcessor(client, step, data);
        }

        if (RiakPluginData.Mode.DELETE == data.mode) {
            return new DeleteProcessor(client, step, data);
        }

        throw new RuntimeException("Unknown mode : " + data.mode);
    }
}
