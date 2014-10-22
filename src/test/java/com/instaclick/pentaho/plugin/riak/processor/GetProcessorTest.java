package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.VClock;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.FetchValue.Response;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.util.BinaryValue;
import com.google.common.collect.Lists;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginException;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowMetaInterface;

public class GetProcessorTest
{
    RiakClient client;
    RiakPlugin plugin;
    RiakPluginData data;

    @Before
    public void setUp()
    {
        client          = mock(RiakClient.class, RETURNS_MOCKS);
        data            = mock(RiakPluginData.class);
        plugin          = mock(RiakPlugin.class);
        data.bucket     = "test_bucket";
        data.bucketType = "test_type";
    }

    @Test
    public void testProcessInvalidKey() throws Exception
    {
        final String value           = "foo";
        final String key             = "bar";
        final Object[] row           = new Object[] {key, value};
        final RowMetaInterface meta  = mock(RowMetaInterface.class);
        final GetProcessor processor = new GetProcessor(client, plugin, data);

        data.keyFieldIndex = 2;
        data.outputRowMeta = meta;

        assertFalse(processor.process(row));
        verify(client, never()).execute(any(FetchValue.class));
    }

    @Test
    public void testPutRowToResolveSiblings() throws Exception
    {
        final String key             = "bar";
        final String sValue1         = "sibling 1 value";
        final String sValue2         = "sibling 2 value";
        final RiakObject sibling1    = new RiakObject();
        final RiakObject sibling2    = new RiakObject();
        final Object[] row           = new Object[] {key, null};
        final Object[] sRow1         = new Object[] {key, sValue1};
        final Object[] sRow2         = new Object[] {key, sValue2};
        final RowSet rowSet          = mock(RowSet.class);
        final VClock vClock          = mock(VClock.class);
        final Response response      = mock(Response.class);
        final RowMetaInterface meta  = mock(RowMetaInterface.class);
        final GetProcessor processor = new GetProcessor(client, plugin, data);

        data.resolver        = "test_resolver";
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;

        sibling1.setValue(BinaryValue.create(sValue1));
        sibling2.setValue(BinaryValue.create(sValue2));

        when(response.getVectorClock()).thenReturn(vClock);
        when(plugin.findOutputRowSet(eq(data.resolver))).thenReturn(rowSet);
        when(response.getValues()).thenReturn(Lists.newArrayList(sibling1, sibling2));

        processor.putRowToResolveSiblings(response, row);
        verify(plugin).putRowTo(eq(meta), eq(sRow1), eq(rowSet));
        verify(plugin).putRowTo(eq(meta), eq(sRow2), eq(rowSet));
    }

    @Test
    public void testPutRowToOutputWhitoutResolver() throws Exception
    {
        final String key              = "bar";
        final String value            = "row value";
        final RiakObject object       = new RiakObject();
        final Object[] row            = new Object[] {key, value};
        final RowSet rowSetNormalFlow = mock(RowSet.class);
        final RowSet rowSetResolver   = mock(RowSet.class);
        final VClock vClock           = mock(VClock.class);
        final Response response       = mock(Response.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver        = null;
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;
        
        object.setValue(BinaryValue.create(value));

        when(response.getVectorClock()).thenReturn(vClock);
        when(response.getValues()).thenReturn(Lists.newArrayList(object));
        when(rowSetResolver.getDestinationStepName()).thenReturn("resolver-foo");
        when(rowSetNormalFlow.getDestinationStepName()).thenReturn("normal-flow");
        when(plugin.getOutputRowSets()).thenReturn(Lists.newArrayList(rowSetNormalFlow, rowSetResolver));

        processor.putRowToOutput(row);
        verify(plugin).putRow(eq(meta), eq(row));
    }

    @Test
    public void testPutRowToOutputUsingResolver() throws Exception
    {
        final String key              = "bar";
        final String value            = "row value";
        final RiakObject object       = new RiakObject();
        final Object[] row            = new Object[] {key, value};
        final RowSet rowSetNormalFlow = mock(RowSet.class);
        final RowSet rowSetResolver   = mock(RowSet.class);
        final VClock vClock           = mock(VClock.class);
        final Response response       = mock(Response.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver        = "resolver-foo";
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;
        
        object.setValue(BinaryValue.create(value));

        when(response.getVectorClock()).thenReturn(vClock);
        when(response.getValues()).thenReturn(Lists.newArrayList(object));
        when(rowSetResolver.getDestinationStepName()).thenReturn("resolver-foo");
        when(rowSetNormalFlow.getDestinationStepName()).thenReturn("normal-flow");
        when(plugin.getOutputRowSets()).thenReturn(Lists.newArrayList(rowSetNormalFlow, rowSetResolver));

        processor.putRowToOutput(row);
        verify(plugin).putRowTo(eq(meta), eq(row), eq(rowSetNormalFlow));
        verify(plugin, never()).putRowTo(eq(meta), eq(row), eq(rowSetResolver));
    }

    @Test
    public void testAddRiakObjectData() throws Exception
    {
        final String key              = "bar";
        final String value            = "row value";
        final byte[] vClockBytes      = new byte[]{};
        final RiakObject object       = new RiakObject();
        final VClock vClock           = mock(VClock.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final Object[] row            = new Object[] {key, null, null};
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver         = "resolver-foo";
        data.outputRowMeta    = meta;
        data.valueFieldIndex  = 1;
        data.vclockFieldIndex = 2;

        object.setValue(BinaryValue.create(value));

        when(vClock.getBytes()).thenReturn(vClockBytes);

        final Object[] result = processor.addRiakObjectData(vClock, object, row);

        assertSame(key, result[0]);
        assertSame(value, result[1]);
        assertSame(vClockBytes, result[2]);
    }

    @Test(expected = RiakPluginException.class)
    public void testPutRowToResolveSiblingsUndefinedResolverException() throws Exception
    {
        final String value           = "foo";
        final String key             = "bar";
        final Response response      = mock(Response.class);
        final Object[] row           = new Object[] {key, value};
        final RowMetaInterface meta  = mock(RowMetaInterface.class);
        final GetProcessor processor = new GetProcessor(client, plugin, data);

        data.resolver      = null;
        data.outputRowMeta = meta;

        processor.putRowToResolveSiblings(response, row);
    }

    @Test(expected = RiakPluginException.class)
    public void testPutRowToResolveSiblingsUnableToFindRowSetResolverException() throws Exception
    {
        final String value           = "foo";
        final String key             = "bar";
        final Response response      = mock(Response.class);
        final Object[] row           = new Object[] {key, value};
        final RowMetaInterface meta  = mock(RowMetaInterface.class);
        final GetProcessor processor = new GetProcessor(client, plugin, data);

        data.resolver      = "undefined-resolver";
        data.outputRowMeta = meta;

        when(plugin.findOutputRowSet(eq(data.resolver))).thenReturn(null);

        processor.putRowToResolveSiblings(response, row);
    }
}
