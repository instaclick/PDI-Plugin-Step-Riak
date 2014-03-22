package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.RawClient;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

public class PutProcessor extends AbstractProcessor
{
    public PutProcessor(final RawClient client, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(client, plugin, data);
    }

    @Override
    public boolean process(final Object[] r) throws Exception
    {
        if ( ! hasRiakKey(r)) {
            return false;
        }

        final String key                = getRiakKey(r);
        final String value              = getRiakValue(r);
        final byte[] vclock             = getRiakVClock(r);
        final RiakObjectBuilder builder = RiakObjectBuilder
            .newBuilder(data.bucket, key)
            .withValue(value);

        if (vclock != null) {
            builder.withVClock(vclock);
        }

        client.store(builder.build());
        plugin.putRow(data.outputRowMeta, r);

        return true;
    }
}
