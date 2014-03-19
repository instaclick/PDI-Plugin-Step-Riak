package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakLink;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.operations.FetchObject;
import com.instaclick.pentaho.plugin.riak.AggregateSiblingsResolver;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import java.util.Collection;
import java.util.List;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowDataUtil;

public class GetProcessor extends AbstractProcessor
{
    public GetProcessor(final Bucket bucket, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(bucket, plugin, data);
    }

    protected void putRowToResolveSiblings(final AggregateSiblingsResolver resolver, final Object[] r) throws Exception
    {
        if (data.resolver == null) {
            throw new RiakException("Conflict resolver step is not defined");
        }

        final String stepName                  = data.resolver;
        final Collection<IRiakObject> siblings = resolver.getSiblings();
        final RowSet rowSet                    = plugin.findOutputRowSet(stepName);

        if (rowSet == null) {
            throw new RiakException("Unable to find conflict resolver step : " + stepName);
        }

        for (IRiakObject sibling : siblings) {
            final String value = sibling.getValueAsString();
            final Object[] row = RowDataUtil.addValueData(r, data.outputRowMeta.size() - 1, value);

            plugin.putRowTo(data.outputRowMeta, row, rowSet);
        }

    }

    protected void putRowToOutput(final Object[] r) throws Exception
    {
        final List<RowSet> rowSetList = plugin.getOutputRowSets();
        final String resolverStep     = data.resolver;

        if (resolverStep == null) {
            plugin.putRow(data.outputRowMeta, r);

            return;
        }

        // @TODO - filter step output target
        for (RowSet rowSet : rowSetList) {

            plugin.logDebug("RowSet name   : " + rowSet.getName());
            plugin.logDebug("Resolver name : " + resolverStep);

            if (resolverStep.equals(rowSet.getName())) {
                continue;
            }

            plugin.logDebug("putRowTo : " + rowSet.getName());

            plugin.putRowTo(data.outputRowMeta, r, rowSet);
        }
    }

    @Override
    public boolean process(Object[] r) throws Exception
    {
        final String key = getRiakKey(r);

        if (key == null) {
            return false;
        }

        final AggregateSiblingsResolver resolver = new AggregateSiblingsResolver();
        final FetchObject<IRiakObject> fetchObject = bucket.fetch(key);
        final IRiakObject object                   = fetchObject
            .withResolver(resolver)
            .execute();

        if (resolver.hasSiblings()) {
            putRowToResolveSiblings(resolver, r);

            if (plugin.isDebug()) {
                plugin.logDebug("Siblings for key : " + key);
            }

            return true;
        }

        if (object == null) {
            putRowToOutput(r);

            return true;
        }

        r = RowDataUtil.addValueData(r, data.valueFieldIndex, object.getValueAsString());

        if (data.vclockFieldIndex != null && fetchObject.getVClock() != null) {
            r = RowDataUtil.addValueData(r, data.vclockFieldIndex, fetchObject.getVClock().asString());
        }

        // put the row to the output row stream
        putRowToOutput(r);

        return true;
    }
}
