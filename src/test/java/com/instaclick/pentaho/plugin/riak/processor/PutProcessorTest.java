package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.google.common.collect.Lists;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginData.Index;
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

        verify(plugin, only()).putRow(eq(rowMeta), eq(row));
    }

    @Test
    public void testProcessPutWithVClock() throws Exception
    {
        final String key                = "riak_key";
        final String value              = "riak_value";
        final byte[] vclock             = "vclock".getBytes();
        final Object[] row              = new Object[] {key, value, vclock};
        final PutProcessor processor    = new PutProcessor(client, plugin, data);

        data.keyFieldIndex    = 0;
        data.valueFieldIndex  = 1;
        data.vclockFieldIndex = 2;
        data.indexes          = new ArrayList<RiakPluginData.Index>();

        when(rowMeta.getString(eq(row), eq(data.keyFieldIndex)))
            .thenReturn(key);

        when(rowMeta.getString(eq(row), eq(data.valueFieldIndex)))
            .thenReturn(value);

        when(rowMeta.getBinary(eq(row), eq(data.vclockFieldIndex)))
            .thenReturn(vclock);

        assertTrue(processor.process(row));

        verify(plugin, times(1)).putRow(eq(rowMeta), eq(row));
        verify(rowMeta).getString(eq(row), eq(data.keyFieldIndex));
        verify(rowMeta).getString(eq(row), eq(data.valueFieldIndex));
        verify(rowMeta).getBinary(eq(row), eq(data.vclockFieldIndex));
    }

    @Test
    public void testProcessPutWithContentType() throws Exception
    {
        final String key                = "riak_key";
        final String value              = "riak_value";
        final String contentType        = "text/plain";
        final Object[] row              = new Object[] {key, value, contentType};
        final PutProcessor processor    = new PutProcessor(client, plugin, data);

        data.keyFieldIndex         = 0;
        data.valueFieldIndex       = 1;
        data.contentTypeFieldIndex = 2;
        data.indexes               = new ArrayList<RiakPluginData.Index>();

        when(rowMeta.getString(eq(row), eq(data.keyFieldIndex)))
            .thenReturn(key);

        when(rowMeta.getString(eq(row), eq(data.valueFieldIndex)))
            .thenReturn(value);

        when(rowMeta.getString(eq(row), eq(data.contentTypeFieldIndex)))
            .thenReturn(contentType);

        assertTrue(processor.process(row));

        verify(plugin, times(1)).putRow(eq(rowMeta), eq(row));
        verify(rowMeta).getString(eq(row), eq(data.keyFieldIndex));
        verify(rowMeta).getString(eq(row), eq(data.valueFieldIndex));
        verify(rowMeta).getString(eq(row), eq(data.contentTypeFieldIndex));
    }

    @Test
    public void testProcessPutWithIndexes() throws Exception
    {
        final String key             = "riak_key";
        final String value           = "riak_value";
        final String string_index    = "string_index";
        final Long index_index       = 100000000000000L;
        final Object[] row           = new Object[] {key, value, string_index, index_index};
        final Index binIndex         = new Index("string_index", 2, "bin");
        final Index intIndex         = new Index("integer_index", 3, "int");
        final PutProcessor processor = new PutProcessor(client, plugin, data);

        data.keyFieldIndex   = 0;
        data.valueFieldIndex = 1;
        data.indexes         = Lists.newArrayList(binIndex, intIndex);

        when(rowMeta.getString(eq(row), eq(data.keyFieldIndex)))
            .thenReturn(key);

        when(rowMeta.getString(eq(row), eq(data.valueFieldIndex)))
            .thenReturn(value);

        when(rowMeta.getInteger(eq(row), eq(intIndex.field)))
            .thenReturn(index_index);

        when(rowMeta.getString(eq(row), eq(binIndex.field)))
            .thenReturn(string_index);

        assertTrue(processor.process(row));

        verify(plugin, times(1)).putRow(eq(rowMeta), eq(row));
        verify(rowMeta).getString(eq(row), eq(data.keyFieldIndex));
        verify(rowMeta).getString(eq(row), eq(data.valueFieldIndex));

        verify(rowMeta).getInteger(eq(row), eq(intIndex.field));
        verify(rowMeta).getString(eq(row), eq(binIndex.field));
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
