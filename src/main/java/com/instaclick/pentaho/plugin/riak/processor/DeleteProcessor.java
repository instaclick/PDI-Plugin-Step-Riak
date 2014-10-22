package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.core.query.Location;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

public class DeleteProcessor extends AbstractProcessor
{
    public DeleteProcessor(final RiakClient client, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(client, plugin, data);
    }

    @Override
    public boolean process(final Object[] r) throws Exception
    {
        if ( ! hasRiakKey(r)) {
            return false;
        }
        
        final Location location = getLocation(r);
        final DeleteValue dv    = new DeleteValue.Builder(location).build();

        client.execute(dv);
        plugin.putRow(data.outputRowMeta, r);

        return true;
    }
}
