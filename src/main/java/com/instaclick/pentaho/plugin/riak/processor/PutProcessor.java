package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.BasicVClock;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.indexes.LongIntIndex;
import com.basho.riak.client.core.query.indexes.RiakIndexes;
import com.basho.riak.client.core.query.indexes.StringBinIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;

public class PutProcessor extends AbstractProcessor
{
    public PutProcessor(final RiakClient client, final RiakPlugin plugin, final RiakPluginData data)
    {
        super(client, plugin, data);
    }

    @Override
    public boolean process(final Object[] r) throws Exception
    {
        if ( ! hasRiakKey(r)) {
            return false;
        }

        final Location location          = getLocation(r);
        final RiakObject object          = new RiakObject();
        final byte[] vclock              = getRiakVClock(r);
        final RiakIndexes indexes        = object.getIndexes();
        final String contentType         = getRiakContentType(r);
        final StoreValue.Builder builder = new StoreValue.Builder(object);

        object.setValue(BinaryValue.create(getRiakValue(r)));
        builder.withLocation(location);

        if (vclock != null) {
            builder.withVectorClock(new BasicVClock(vclock));
        }

        if (contentType != null) {
            object.setContentType(contentType);
        }

        for (final RiakPluginData.Index item : data.indexes) {

            if ("int".endsWith(item.type)) {
                final Long value             = getIntegerRowValue(r, item.field);
                final LongIntIndex index     = indexes.getIndex(LongIntIndex.named(item.index));

                index.add(value);

                continue;
            }

            final String value         = getStringRowValue(r, item.field);
            final StringBinIndex index = indexes.getIndex(StringBinIndex.named(item.index));

            index.add(value);
        }

        client.execute(builder.build());
        plugin.putRow(data.outputRowMeta, r);

        return true;
    }
}
