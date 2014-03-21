package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.bucket.Bucket;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

abstract class AbstractProcessor implements Processor
{
    protected final RiakPluginData data;
    protected final RiakPlugin plugin;
    protected final Bucket bucket;

    public AbstractProcessor(final Bucket bucket, final RiakPlugin plugin, final RiakPluginData data)
    {
        this.plugin = plugin;
        this.bucket = bucket;
        this.data   = data;
    }

    protected String getRiakKey(final Object[] r) throws Exception
    {
        if (r.length > data.keyFieldIndex) {
            return (r[data.keyFieldIndex] == null) ? null : r[data.keyFieldIndex].toString();
        }

        final String message = plugin.getLinesRead() + " - Invalid key row";

        if (plugin.isDebug()) {
            plugin.logDebug(message);
        }

        plugin.putError(plugin.getInputRowMeta(), r, 1, message, null, "ICRiakPlugin001");

        return null;
    }

    protected String getRiakValue(final Object[] r) throws Exception
    {
        if (r.length > data.valueFieldIndex) {
            return (r[data.valueFieldIndex] == null) ? null : r[data.valueFieldIndex].toString();
        }

        final String message = plugin.getLinesRead() + " - Invalid value row";

        if (plugin.isDebug()) {
            plugin.logDebug(message);
        }

        plugin.putError(plugin.getInputRowMeta(), r, 1, message, null, "ICRiakPlugin002");

        return null;
    }
}
