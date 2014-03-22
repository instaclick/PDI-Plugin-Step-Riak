package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.raw.RawClient;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.pentaho.di.core.row.RowMetaInterface;

public class PutProcessorTest
{
    RawClient client;
    RiakPlugin plugin;
    RiakPluginData data;

    @Before
    public void setUp()
    {
        client      = mock(RawClient.class, RETURNS_MOCKS);
        data        = mock(RiakPluginData.class);
        plugin      = mock(RiakPlugin.class);
        data.bucket = "test_bucket";
    }

    @Test
    public void testProcessBasicPut() throws Exception
    {
        final String key                = "riak_key";
        final String value              = "riak_value";
        final Object[] row              = new Object[] {key, value};
        final RowMetaInterface meta     = mock(RowMetaInterface.class);
        final PutProcessor processor    = new PutProcessor(client, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;
        data.outputRowMeta   = meta;

        assertTrue(processor.process(row));

        verify(client, only()).store(any(IRiakObject.class));
        verify(plugin, only()).putRow(eq(meta), eq(row));
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
        verify(client, never()).delete(eq(data.bucket), eq(key));
    }
}
