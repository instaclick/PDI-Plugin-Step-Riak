package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginException;
import java.util.List;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowDataUtil;

public class GetProcessor extends AbstractProcessor
{
    public GetProcessor(final RawClient client, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(client, plugin, data);
    }

    protected void putRowToResolveSiblings(final RiakResponse response, final Object[] r) throws Exception
    {
        if (data.resolver == null) {
            throw new RiakPluginException("Conflict resolver step is not defined");
        }

        final String stepName        = data.resolver;
        final IRiakObject[] siblings = response.getRiakObjects();
        final RowSet rowSet          = plugin.findOutputRowSet(stepName);

        if (rowSet == null) {
            throw new RiakPluginException("Unable to find conflict resolver step : " + stepName);
        }

        for (IRiakObject sibling : siblings) {
            plugin.putRowTo(data.outputRowMeta, addRiakObjectData(response.getVclock(), sibling, r.clone()), rowSet);
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

        for (RowSet rowSet : rowSetList) {
            if (resolverStep.equals(rowSet.getDestinationStepName())) {
                continue;
            }

            plugin.putRowTo(data.outputRowMeta, r, rowSet);
        }
    }

    protected Object[] addRiakObjectData(final VClock vClock, final IRiakObject object, Object[] r) throws Exception
    {
        r = RowDataUtil.addValueData(r, data.valueFieldIndex, object.getValueAsString());

        if (data.vclockFieldIndex != null && vClock != null) {
            r = RowDataUtil.addValueData(r, data.vclockFieldIndex, vClock.getBytes());
        }

        return r;
    }

    @Override
    public boolean process(Object[] r) throws Exception
    {
        if ( ! hasRiakKey(r)) {
            return false;
        }

        final String key            = getRiakKey(r);
        final RiakResponse response = client.fetch(data.bucket, key);

        if (response == null || ! response.hasValue()) {
            putRowToOutput(r);

            return true;
        }

        if (response.hasSiblings()) {

            if (plugin.isDebug()) {
                plugin.logDebug("'" + key + "' has siblings");
            }

            putRowToResolveSiblings(response, r);

            return true;
        }

        putRowToOutput(addRiakObjectData(response.getVclock(), response.getRiakObjects()[0], r));

        return true;
    }
}
