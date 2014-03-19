package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.bucket.Bucket;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

public class PutProcessor extends AbstractProcessor
{
    public PutProcessor(final Bucket bucket, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(bucket, plugin, data);
    }

    @Override
    public boolean process(final Object[] r) throws Exception
    {
        final String key   = getRiakKey(r);
        final String value = getRiakValue(r);

        if (key == null) {
            return false;
        }

        bucket.store(key, value).execute();
        plugin.putRow(data.outputRowMeta, r);

        return true;
    }
}
