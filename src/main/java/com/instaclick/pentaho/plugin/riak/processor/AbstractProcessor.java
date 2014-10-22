package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

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
        if (hasRiakKey(r)) {
            return (r[data.keyFieldIndex] == null) ? null : r[data.keyFieldIndex].toString();
        }

        logUndefinedRow(r, "Invalid key row", "ICRiakPlugin001");

        return null;
    }

    protected String getRiakValue(final Object[] r) throws Exception
    {
        if (hasRiakValue(r)) {
            return (r[data.valueFieldIndex] == null) ? null : r[data.valueFieldIndex].toString();
        }

        logUndefinedRow(r, "Invalid value row", "ICRiakPlugin002");

        return null;
    }

    protected byte[] getRiakVClock(final Object[] r) throws Exception
    {
        if (hasRiakVClock(r)) {
            return ((r[data.vclockFieldIndex] == null) ? null : (byte[]) r[data.vclockFieldIndex]);
        }

        return null;
    }

    protected boolean hasRiakVClock(final Object[] r) throws Exception
    {
        return rowContains(r, data.vclockFieldIndex);
    }

    protected boolean hasRiakKey(final Object[] r) throws Exception
    {
        return rowContains(r, data.keyFieldIndex);
    }

    protected boolean hasRiakValue(final Object[] r) throws Exception
    {
        return rowContains(r, data.valueFieldIndex);
    }

    protected boolean rowContains(final Object[] r, final Integer index) throws Exception
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
