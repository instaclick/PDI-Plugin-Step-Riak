package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import java.util.ArrayList;
import java.util.List;

abstract class AbstractProcessor implements Processor
{
    protected final RiakPluginData data;
    protected final Namespace namespace;
    protected final RiakPlugin plugin;
    protected final RiakClient client;

    public AbstractProcessor(final RiakClient client, final RiakPlugin plugin, final RiakPluginData data)
    {
        this.data       = data;
        this.plugin     = plugin;
        this.client     = client;
        this.namespace  = new Namespace(data.bucketType, data.bucket);
    }
    
    protected Location getLocation(final Object[] r) throws Exception
    {
        final String key        = getRiakKey(r);
        final Location location = new Location(namespace, key);

        return location;
    }

    protected String getRiakKey(final Object[] r) throws Exception
    {
        if ( ! hasRiakKey(r)) {
            logUndefinedRow(r, "Invalid key row", "ICRiakPlugin001");
        }

        return getStringRowValue(r, data.keyFieldIndex);
    }

    protected String getRiakValue(final Object[] r) throws Exception
    {
        if ( ! rowValueContains(r, data.valueFieldIndex)) {
            logUndefinedRow(r, "Invalid value row", "ICRiakPlugin002");
        }

        return getStringRowValue(r, data.valueFieldIndex);
    }

    protected byte[] getRiakVClock(final Object[] r) throws Exception
    {
        return getBinaryRowValue(r, data.vclockFieldIndex);
    }

    protected String getRiakContentType(final Object[] r) throws Exception
    {
        return getStringRowValue(r, data.contentTypeFieldIndex);
    }

    protected boolean hasRiakKey(final Object[] r) throws Exception
    {
        return rowValueContains(r, data.keyFieldIndex);
    }

    protected Long getIntegerRowValue(final Object[] r, final Integer index) throws Exception
    {
        if ( ! rowValueContains(r, index)) {
            return null;
        }

        return data.outputRowMeta.getInteger(r, index);
    }

    protected String getStringRowValue(final Object[] r, final Integer index) throws Exception
    {
        if ( ! rowValueContains(r, index)) {
            return null;
        }

        return data.outputRowMeta.getString(r, index);
    }

    protected byte[] getBinaryRowValue(final Object[] r, final Integer index) throws Exception
    {
        if ( ! rowValueContains(r, index)) {
            return null;
        }

        return data.outputRowMeta.getBinary(r, index);
    }

    protected boolean rowValueContains(final Object[] r, final Integer index) throws Exception
    {
        if (index == null || index < 0) {
            return false;
        }

        return (r.length > index);
    }

    protected void logUndefinedRow(final Object[] r, final String log, final String code) throws Exception
    {
        final String msg = plugin.getLinesRead() + " - " + log;

        if (plugin.isDebug()) {
            plugin.logDebug(msg);
        }

        plugin.putError(plugin.getInputRowMeta(), r, 1, msg, null, code);
    }

    @Override
    public void shutdown()
    {
        client.shutdown();
    }
}
