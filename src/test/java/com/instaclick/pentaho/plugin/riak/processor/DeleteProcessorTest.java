package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.bucket.Bucket;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.pentaho.di.core.row.RowMetaInterface;

public class DeleteProcessorTest
{
    Bucket bucket;
    RiakPlugin plugin; 
    RiakPluginData data;

    @Before
    public void setUp()
    {
        plugin = mock(RiakPlugin.class);
        data   = mock(RiakPluginData.class);
        bucket = mock(Bucket.class, RETURNS_MOCKS);
    }

    @Test
    public void testProcessBasicDelete() throws Exception
    {
        final String value              = null;
        final String key                = "riak_key";
        final Object[] row              = new Object[] {key, value};
        final RowMetaInterface meta     = mock(RowMetaInterface.class);
        final DeleteProcessor processor = new DeleteProcessor(bucket, plugin, data);

        data.keyFieldIndex = 0;
        data.outputRowMeta = meta;

        assertTrue(processor.process(row));

        verify(bucket, only()).delete(eq(key));
        verify(plugin, only()).putRow(eq(meta), eq(row));
    }

    @Test
    public void testProcessInvalidKey() throws Exception
    {
        final String value              = null;
        final String key                = null;
        final Object[] row              = new Object[] {key, value};
        final RowMetaInterface meta     = mock(RowMetaInterface.class);
        final DeleteProcessor processor = new DeleteProcessor(bucket, plugin, data);

        data.keyFieldIndex = 2;
        data.outputRowMeta = meta;

        assertFalse(processor.process(row));

        verify(bucket, never()).delete(eq(key));
    }
}
