package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginException;
import java.util.List;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowDataUtil;

public class GetProcessor extends AbstractProcessor
{
    public GetProcessor(final RiakClient client, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(client, plugin, data);
    }

    protected void putRowToResolveSiblings(final FetchValue.Response response, final Object[] r) throws Exception
    {
        if (data.resolver == null) {
            throw new RiakPluginException("Conflict resolver step is not defined");
        }
        
        final String stepName           = data.resolver;
        final List<RiakObject> siblings = response.getValues();
        final VClock vClock             = response.getVectorClock();
        final RowSet rowSet             = plugin.findOutputRowSet(stepName);

        if (rowSet == null) {
            throw new RiakPluginException("Unable to find conflict resolver step : " + stepName);
        }

        for (final RiakObject sibling : siblings) {
            plugin.putRowTo(data.outputRowMeta, addRiakObjectData(vClock, sibling, r.clone()), rowSet);
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

    protected Object[] addRiakObjectData(final VClock vClock, final RiakObject object, Object[] r) throws Exception
    {
        r = RowDataUtil.addValueData(r, data.valueFieldIndex, object.getValue());

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

        final Location location            = getLocation(r);
        final FetchValue fv                = new FetchValue.Builder(location).build();
        final FetchValue.Response response = client.execute(fv);

        if (response == null || response.isNotFound()) {
            putRowToOutput(r);

            return true;
        }

        if (response.getNumberOfValues() > 1) {

            if (plugin.isDebug()) {
                plugin.logDebug("'" + location + "' has siblings");
            }

            putRowToResolveSiblings(response, r);

            return true;
        }

        putRowToOutput(addRiakObjectData(response.getVectorClock(), response.getValues().get(0), r));

        return true;
    }
}
