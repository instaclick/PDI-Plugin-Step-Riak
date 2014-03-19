package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.raw.pbc.PBClientConfig;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginMeta;

public class ProcessorFactory
{
    private IRiakClient clientFor(final RiakPluginData data) throws RiakException
    {
        return RiakFactory.newClient(new PBClientConfig.Builder()
            .withHost(data.host)
            .withPort(data.port)
            .build());
    }

    private Bucket bucketFor(final IRiakClient client, final RiakPluginData data) throws RiakException
    {
        return client.fetchBucket(data.bucket).execute();
    }

    public Processor processorFor(final RiakPlugin step,final RiakPluginData data, final RiakPluginMeta meta) throws RiakException
    {
        final IRiakClient client = clientFor(data);
        final Bucket bucket      = bucketFor(client, data);

        if (RiakPluginData.Mode.GET == data.mode) {
            return new GetProcessor(bucket, step, data);
        }

        if (RiakPluginData.Mode.PUT == data.mode) {
            return new PutProcessor(bucket, step, data);
        }

        if (RiakPluginData.Mode.DELETE == data.mode) {
            return new DeleteProcessor(bucket, step, data);
        }

        throw new RuntimeException("Unknown mode : " + data.mode);
    }
}
