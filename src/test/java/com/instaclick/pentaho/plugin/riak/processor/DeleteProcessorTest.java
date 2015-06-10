package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.pentaho.di.core.row.RowMetaInterface;

public class DeleteProcessorTest
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
    public void testProcessBasicDelete() throws Exception
    {
        final String value              = null;
        final String key                = "riak_key";
        final Object[] row              = new Object[] {key, value};
        final DeleteProcessor processor = new DeleteProcessor(client, plugin, data);

        data.keyFieldIndex = 0;

        when(rowMeta.getString(eq(row), eq(data.keyFieldIndex)))
            .thenReturn(key);

        assertTrue(processor.process(row));

        //verify(client, only()).delete(eq(data.bucket), eq(key));
        verify(plugin, only()).putRow(eq(rowMeta), eq(row));
    }

    @Test
    public void testProcessInvalidKey() throws Exception
    {
        final String value              = null;
        final String key                = null;
        final Object[] row              = new Object[] {key, value};
        final RowMetaInterface meta     = mock(RowMetaInterface.class);
        final DeleteProcessor processor = new DeleteProcessor(client, plugin, data);

        data.keyFieldIndex = 2;
        data.outputRowMeta = meta;

        assertFalse(processor.process(row));

        //verify(client, never()).delete(eq(data.bucket), eq(key));
    }
}
