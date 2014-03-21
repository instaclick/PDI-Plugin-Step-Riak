
package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.bucket.Bucket;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import org.pentaho.di.core.row.RowMetaInterface;

public class AbstractProcessorTest
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
    public void testGetRiakKey() throws Exception
    {
        final String value                  = null;
        final String key                    = "riak_key";
        final Object[] row                  = new Object[] {key, value};
        final AbstractProcessor processor   = new AbstractProcessorImpl(bucket, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;

        assertEquals(key, processor.getRiakKey(row));
    }

    @Test
    public void testGetRiakValue() throws Exception
    {
        final String key                    = "riak_key";
        final String value                  = "riak_value";
        final Object[] row                  = new Object[] {key, value};
        final AbstractProcessor processor   = new AbstractProcessorImpl(bucket, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;

        assertEquals(value, processor.getRiakValue(row));
    }

    @Test
    public void testGetInvalidRiakKey() throws Exception
    {
        final String key                    = "riak_key";
        final Object[] row                  = new Object[] {key};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final AbstractProcessor processor   = new AbstractProcessorImpl(bucket, plugin, data);

        data.keyFieldIndex   = 3;
        data.valueFieldIndex = 1;

        when(plugin.isDebug()).thenReturn(true);
        when(plugin.getLinesRead()).thenReturn(1L);
        when(plugin.getInputRowMeta()).thenReturn(meta);

        assertNull(processor.getRiakKey(row));
        verify(plugin).putError(eq(meta), eq(row), eq(1L), eq("1 - Invalid key row"), isNull(String.class), eq("ICRiakPlugin001"));
    }

    @Test
    public void testGetInvalidRiakValue() throws Exception
    {
        final String key                    = "riak_key";
        final String value                  = "riak_value";
        final Object[] row                  = new Object[] {key, value};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final AbstractProcessor processor   = new AbstractProcessorImpl(bucket, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 2;

        when(plugin.isDebug()).thenReturn(true);
        when(plugin.getLinesRead()).thenReturn(1L);
        when(plugin.getInputRowMeta()).thenReturn(meta);

        assertNull(processor.getRiakValue(row));
        verify(plugin).putError(eq(meta), eq(row), eq(1L), eq("1 - Invalid value row"), isNull(String.class), eq("ICRiakPlugin002"));
    }

    @Test
    public void testGetNullKeyValue() throws Exception
    {
        final Object[] row                  = new Object[] {null, null};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final AbstractProcessor processor   = new AbstractProcessorImpl(bucket, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;

        when(plugin.isDebug()).thenReturn(true);
        when(plugin.getLinesRead()).thenReturn(1L);
        when(plugin.getInputRowMeta()).thenReturn(meta);

        assertNull(processor.getRiakKey(row));
        assertNull(processor.getRiakValue(row));
        verify(plugin, never()).putError(any(RowMetaInterface.class), any(Object[].class), any(Long.class), any(String.class), any(String.class), any(String.class));
    }

    public class AbstractProcessorImpl extends AbstractProcessor
    {
        public AbstractProcessorImpl(final Bucket bucket, final RiakPlugin plugin, final RiakPluginData data)
        {
            super(bucket, plugin, data);
        }

        @Override
        public boolean process(Object[] r) throws Exception
        {
            return true;
        }
    }
}
