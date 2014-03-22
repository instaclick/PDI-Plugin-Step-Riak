package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.raw.RawClient;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

public class DeleteProcessor extends AbstractProcessor
{
    public DeleteProcessor(final RawClient client, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(client, plugin, data);
    }

    @Override
    public boolean process(final Object[] r) throws Exception
    {
        if ( ! hasRiakKey(r)) {
            return false;
        }

        client.delete(data.bucket, getRiakKey(r));
        plugin.putRow(data.outputRowMeta, r);

        return true;
    }
}
