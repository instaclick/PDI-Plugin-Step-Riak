package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.VClock;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.google.common.collect.Lists;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginException;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import org.junit.Before;
import org.pentaho.di.core.RowSet;
import org.pentaho.di.core.row.RowMetaInterface;

public class GetProcessorTest
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
        verify(client, never()).delete(eq(data.bucket), eq(key));
    }

    @Test
    public void testPutRowToResolveSiblings() throws Exception
    {
        final String key             = "bar";
        final String sValue1         = "sibling 1 value";
        final String sValue2         = "sibling 2 value";
        final Object[] row           = new Object[] {key, null};
        final Object[] sRow1         = new Object[] {key, sValue1};
        final Object[] sRow2         = new Object[] {key, sValue2};
        final RowSet rowSet          = mock(RowSet.class);
        final VClock vClock          = mock(VClock.class);
        final IRiakObject sibling1   = mock(IRiakObject.class);
        final IRiakObject sibling2   = mock(IRiakObject.class);
        final RiakResponse response  = mock(RiakResponse.class);
        final RowMetaInterface meta  = mock(RowMetaInterface.class);
        final GetProcessor processor = new GetProcessor(client, plugin, data);

        data.resolver        = "test_resolver";
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;

        when(response.getVclock()).thenReturn(vClock);
        when(sibling1.getValueAsString()).thenReturn(sValue1);
        when(sibling2.getValueAsString()).thenReturn(sValue2);
        when(plugin.findOutputRowSet(eq(data.resolver))).thenReturn(rowSet);
        when(response.getRiakObjects()).thenReturn(new IRiakObject[]{
            sibling1, sibling2
        });

        processor.putRowToResolveSiblings(response, row);
        verify(plugin).putRowTo(eq(meta), eq(sRow1), eq(rowSet));
        verify(plugin).putRowTo(eq(meta), eq(sRow2), eq(rowSet));
    }

    @Test
    public void testPutRowToOutputWhitoutResolver() throws Exception
    {
        final String key              = "bar";
        final String value            = "row value";
        final Object[] row            = new Object[] {key, value};
        final RowSet rowSetNormalFlow = mock(RowSet.class);
        final RowSet rowSetResolver   = mock(RowSet.class);
        final VClock vClock           = mock(VClock.class);
        final IRiakObject object      = mock(IRiakObject.class);
        final RiakResponse response   = mock(RiakResponse.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver        = null;
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;

        when(response.getVclock()).thenReturn(vClock);
        when(object.getValueAsString()).thenReturn(value);
        when(response.getRiakObjects()).thenReturn(new IRiakObject[]{object});
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
        final Object[] row            = new Object[] {key, value};
        final RowSet rowSetNormalFlow = mock(RowSet.class);
        final RowSet rowSetResolver   = mock(RowSet.class);
        final VClock vClock           = mock(VClock.class);
        final IRiakObject object      = mock(IRiakObject.class);
        final RiakResponse response   = mock(RiakResponse.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver        = "resolver-foo";
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;

        when(response.getVclock()).thenReturn(vClock);
        when(object.getValueAsString()).thenReturn(value);
        when(response.getRiakObjects()).thenReturn(new IRiakObject[]{object});
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
        final Object[] row            = new Object[] {key, null, null};
        final VClock vClock           = mock(VClock.class);
        final IRiakObject object      = mock(IRiakObject.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver         = "resolver-foo";
        data.outputRowMeta    = meta;
        data.valueFieldIndex  = 1;
        data.vclockFieldIndex = 2;

        when(vClock.getBytes()).thenReturn(vClockBytes);
        when(object.getValueAsString()).thenReturn(value);

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
        final Object[] row           = new Object[] {key, value};
        final RiakResponse response  = mock(RiakResponse.class);
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
        final Object[] row           = new Object[] {key, value};
        final RiakResponse response  = mock(RiakResponse.class);
        final RowMetaInterface meta  = mock(RowMetaInterface.class);
        final GetProcessor processor = new GetProcessor(client, plugin, data);

        data.resolver      = "undefined-resolver";
        data.outputRowMeta = meta;

        when(plugin.findOutputRowSet(eq(data.resolver))).thenReturn(null);

        processor.putRowToResolveSiblings(response, row);
    }

}
