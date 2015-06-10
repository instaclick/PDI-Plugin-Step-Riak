
package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import static org.mockito.Mockito.*;
import org.pentaho.di.core.row.RowMetaInterface;

public class AbstractProcessorTest
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
    public void testGetRiakKey() throws Exception
    {
        final String value                  = null;
        final String key                    = "riak_key";
        final Object[] row                  = new Object[] {key, value};
        final AbstractProcessor processor   = new AbstractProcessorImpl(client, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;

        when(rowMeta.getString(eq(row), eq(data.keyFieldIndex)))
            .thenReturn(key);

        assertEquals(key, processor.getRiakKey(row));
    }

    @Test
    public void testGetRiakValue() throws Exception
    {
        final String key                    = "riak_key";
        final String value                  = "riak_value";
        final Object[] row                  = new Object[] {key, value};
        final AbstractProcessor processor   = new AbstractProcessorImpl(client, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;

        when(rowMeta.getString(eq(row), eq(data.valueFieldIndex)))
            .thenReturn(value);

        assertEquals(value, processor.getRiakValue(row));
    }

    @Test
    public void testGetInvalidRiakKey() throws Exception
    {
        final String key                    = "riak_key";
        final Object[] row                  = new Object[] {key};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final AbstractProcessor processor   = new AbstractProcessorImpl(client, plugin, data);

        data.keyFieldIndex   = 3;
        data.valueFieldIndex = 1;

        when(plugin.isDebug()).thenReturn(true);
        when(plugin.getLinesRead()).thenReturn(1L);
        when(plugin.getInputRowMeta()).thenReturn(meta);

        assertNull(processor.getRiakKey(row));
        verify(plugin).putError(eq(meta), eq(row), eq(1L), eq("1 - Invalid key row"), isNull(String.class), eq("ICRiakPlugin001"));
    }

    public void testGetInvalidRiakValue() throws Exception
    {
        final String key                    = "riak_key";
        final String value                  = "riak_value";
        final Object[] row                  = new Object[] {key, value};
        final RowMetaInterface meta         = mock(RowMetaInterface.class);
        final AbstractProcessor processor   = new AbstractProcessorImpl(client, plugin, data);

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
        final AbstractProcessor processor   = new AbstractProcessorImpl(client, plugin, data);

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
        public AbstractProcessorImpl(final RiakClient client, final RiakPlugin plugin, final RiakPluginData data)
        {
            super(client, plugin, data);
        }

        @Override
        public boolean process(Object[] r) throws Exception
        {
            return true;
        }
    }
}
