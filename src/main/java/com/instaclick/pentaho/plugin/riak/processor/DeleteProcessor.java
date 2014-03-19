package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.bucket.Bucket;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

public class DeleteProcessor extends AbstractProcessor
{
    public DeleteProcessor(final Bucket bucket, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(bucket, plugin, data);
    }

    @Override
    public boolean process(final Object[] r) throws Exception
    {
        final String key = getRiakKey(r);

        if (key == null) {
            return false;
        }

        bucket.delete(key).execute();
        plugin.putRow(data.outputRowMeta, r);

        return true;
    }
}
