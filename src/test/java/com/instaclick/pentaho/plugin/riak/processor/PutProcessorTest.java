package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.bucket.Bucket;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.pentaho.di.core.row.RowMetaInterface;

public class PutProcessorTest
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
    public void testProcessBasicPut() throws Exception
    {
        final String key                = "riak_key";
        final String value              = "riak_value";
        final Object[] row              = new Object[] {key, value};
        final RowMetaInterface meta     = mock(RowMetaInterface.class);
        final PutProcessor processor    = new PutProcessor(bucket, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;
        data.outputRowMeta   = meta;

        assertTrue(processor.process(row));

        verify(bucket, only()).store(eq(key), eq(value));
        verify(plugin, only()).putRow(eq(meta), eq(row));
    }

    @Test
    public void testProcessInvalidKey() throws Exception
    {
        final String value              = null;
        final String key                = null;
        final Object[] row              = new Object[] {key, value};
        final RowMetaInterface meta     = mock(RowMetaInterface.class);
        final PutProcessor processor    = new PutProcessor(bucket, plugin, data);

        data.keyFieldIndex   = 3;
        data.valueFieldIndex = 1;
        data.outputRowMeta   = meta;

        assertFalse(processor.process(row));
        verify(bucket, never()).delete(eq(key));
    }
}
