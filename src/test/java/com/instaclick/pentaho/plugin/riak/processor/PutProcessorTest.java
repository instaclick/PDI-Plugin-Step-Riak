package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.pentaho.di.core.row.RowMetaInterface;

public class PutProcessorTest
{
    RiakClient client;
    RiakPlugin plugin;
    RiakPluginData data;
    RowMetaInterface rowMeta;

    @Before
    public void setUp()
    {
        client              = mock(RiakClient.class, RETURNS_MOCKS);
        rowMeta             = mock(RowMetaInterface.class);
        data                = mock(RiakPluginData.class);
        plugin              = mock(RiakPlugin.class);
        data.bucket         = "test_bucket";
        data.bucketType     = "test_type";
        data.outputRowMeta  = rowMeta;
    }

    @Test
    public void testProcessBasicPut() throws Exception
    {
        final String key                = "riak_key";
        final String value              = "riak_value";
        final Object[] row              = new Object[] {key, value};
        final PutProcessor processor    = new PutProcessor(client, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;
        data.indexes         = new ArrayList<RiakPluginData.Index>();

        when(rowMeta.getString(eq(row), eq(data.keyFieldIndex)))
            .thenReturn(key);

        when(rowMeta.getString(eq(row), eq(data.valueFieldIndex)))
            .thenReturn(value);

        assertTrue(processor.process(row));

        //verify(client, only()).store(any(RiakObject.class));
        verify(plugin, only()).putRow(eq(rowMeta), eq(row));
    }

    @Test
    public void testProcessInvalidKey() throws Exception
    {
        final String value              = null;
        final String key                = null;
        final Object[] row              = new Object[] {key, value};
        final RowMetaInterface meta     = mock(RowMetaInterface.class);
        final PutProcessor processor    = new PutProcessor(client, plugin, data);

        data.keyFieldIndex   = 3;
        data.valueFieldIndex = 1;
        data.outputRowMeta   = meta;

        assertFalse(processor.process(row));
        //verify(client, never()).delete(eq(data.bucket), eq(key));
    }
}
