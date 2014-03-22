package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginMeta;
import java.io.IOException;

public class ProcessorFactory
{
    private RawClient clientFor(final RiakPluginData data) throws IOException
    {
        return new PBClientAdapter(data.host, data.port);
    }

    public Processor processorFor(final RiakPlugin step,final RiakPluginData data, final RiakPluginMeta meta) throws IOException
    {
        final RawClient client = clientFor(data);

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
