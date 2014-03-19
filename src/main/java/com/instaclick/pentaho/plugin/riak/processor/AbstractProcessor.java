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
        if (r.length < data.keyFieldIndex || r[data.keyFieldIndex] == null) {
            String putErrorMessage = plugin.getLinesRead() + " - Ignore invalid key row";

            if (plugin.isDebug()) {
                plugin.logDebug(putErrorMessage);
            }

            plugin.putError(plugin.getInputRowMeta(), r, 1, putErrorMessage, null, "ICRiakPlugin001");

            return null;
        }

        return String.valueOf(r[data.keyFieldIndex]);
    }

    protected String getRiakValue(final Object[] r) throws Exception
    {
        if (r.length < data.valueFieldIndex || r[data.valueFieldIndex] == null) {
            String putErrorMessage = plugin.getLinesRead() + " - Ignore invalid value row";

            if (plugin.isDebug()) {
                plugin.logDebug(putErrorMessage);
            }

            plugin.putError(plugin.getInputRowMeta(), r, 1, putErrorMessage, null, "ICRiakPlugin002");

            return null;
        }

        return String.valueOf(r[data.valueFieldIndex]);
    }
}
