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
import com.instaclick.pentaho.plugin.riak.Whitebox;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
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
        final BinaryValue sValue1    = BinaryValue.create("sibling 1 value");
        final BinaryValue sValue2    = BinaryValue.create("sibling 2 value");
        final RiakObject sibling1    = new RiakObject();
        final RiakObject sibling2    = new RiakObject();
        final Object[] row           = new Object[] {key, null};
        final Object[] sRow1         = new Object[] {key, sValue1};
        final Object[] sRow2         = new Object[] {key, sValue2};
        final RowSet rowSet          = mock(RowSet.class);
        final VClock vClock           = mock(VClock.class);
        final Response response       = mock(Response.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final List<RiakObject> values = Lists.newArrayList(sibling1, sibling2);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver        = "test_resolver";
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;

        sibling1.setVClock(vClock);
        sibling2.setVClock(vClock);
        sibling1.setValue(sValue1);
        sibling2.setValue(sValue2);

        Whitebox.seResponsetFieldValue(response, "values", values);

        when(plugin.findOutputRowSet(eq(data.resolver))).thenReturn(rowSet);

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
        final VClock vClock           = mock(VClock.class);
        final Object[] row            = new Object[] {key, value};
        final RowSet rowSetNormalFlow = mock(RowSet.class);
        final RowSet rowSetResolver   = mock(RowSet.class);
        final Response response       = mock(Response.class);
        final List<RiakObject> values = Lists.newArrayList(object);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);

        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver      = "undefined-resolver";
        data.outputRowMeta = meta;

        Whitebox.seResponsetFieldValue(response, "values", values);

        data.resolver        = null;
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;

        object.setValue(BinaryValue.create(value));
        object.setVClock(vClock);

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
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver        = "resolver-foo";
        data.outputRowMeta   = meta;
        data.valueFieldIndex = 1;

        object.setValue(BinaryValue.create(value));

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
        final byte[] vClockBytes      = new byte[]{};
        final RiakObject object       = new RiakObject();
        final VClock vClock           = mock(VClock.class);
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final Object[] row            = new Object[] {key, null, null};
        final BinaryValue value       = BinaryValue.create("row value");
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        data.resolver         = "resolver-foo";
        data.outputRowMeta    = meta;
        data.valueFieldIndex  = 1;
        data.vclockFieldIndex = 2;

        object.setValue(value);

        when(vClock.getBytes()).thenReturn(vClockBytes);

        final Object[] result = processor.addRiakObjectData(vClock, object, row);

        assertEquals(key, result[0]);
        assertEquals(value, result[1]);
        assertEquals(vClockBytes, result[2]);
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
        final String value            = "foo";
        final String key              = "bar";
        final RiakObject sibling1     = new RiakObject();
        final RiakObject sibling2     = new RiakObject();
        final VClock vClock           = mock(VClock.class);
        final Response response       = mock(Response.class);
        final Object[] row            = new Object[] {key, value};
        final RowMetaInterface meta   = mock(RowMetaInterface.class);
        final List<RiakObject> values = Lists.newArrayList(sibling1, sibling2);
        final GetProcessor processor  = new GetProcessor(client, plugin, data);

        sibling1.setVClock(vClock);
        sibling2.setVClock(vClock);

        data.resolver      = "undefined-resolver";
        data.outputRowMeta = meta;

        Whitebox.seResponsetFieldValue(response, "values", values);

        when(plugin.findOutputRowSet(eq(data.resolver))).thenReturn(null);

        processor.putRowToResolveSiblings(response, row);
    }
}
