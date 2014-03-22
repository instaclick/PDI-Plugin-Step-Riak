package com.instaclick.pentaho.plugin.riak;

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import static com.instaclick.pentaho.plugin.riak.Messages.getString;
import com.instaclick.pentaho.plugin.riak.processor.Processor;
import com.instaclick.pentaho.plugin.riak.processor.ProcessorFactory;
import java.io.IOException;

public class RiakPlugin extends BaseStep implements StepInterface
{
    final private ProcessorFactory factory = new ProcessorFactory();
    private Processor processor;
    private RiakPluginData data;
    private RiakPluginMeta meta;

    public RiakPlugin(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans)
    {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException, KettleStepException
    {
        meta       = (RiakPluginMeta) smi;
        data       = (RiakPluginData) sdi;
        Object[] r = getRow();

        if (r == null) {
            setOutputDone();

            return false;
        }

        if (first) {
            first = false;

            try {
                initPlugin();
            } catch (IOException e) {
                throw new RiakPluginException(e.getMessage(), e);
            }
        }

        try {
            return processor.process(r);
        } catch (Exception ex) {
            throw new RiakPluginException(ex.getMessage(), ex);
        }
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (RiakPluginMeta) smi;
        data = (RiakPluginData) sdi;

        return super.init(smi, sdi);
    }

    /**
     * Initialize
     */
    private void initPlugin() throws KettleStepException, IOException
    {
        // clone the input row structure and place it in our data object
        data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
        // use meta.getFields() to change it, so it reflects the output row structure
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this);

        final String resolver           = environmentSubstitute(meta.getResolver());
        final String bucket             = environmentSubstitute(meta.getBucket());
        final String vclock             = environmentSubstitute(meta.getVClock());
        final String host               = environmentSubstitute(meta.getHost());
        final String port               = environmentSubstitute(meta.getPort());
        final String value              = environmentSubstitute(meta.getValue());
        final String key                = environmentSubstitute(meta.getKey());
        final RiakPluginData.Mode mode  = meta.getMode();

        logMinimal(getString("RiakPlugin.Mode.Label")   + " : '" + mode   + "'");
        logMinimal(getString("RiakPlugin.Host.Label")   + " : '" + host   + "'");
        logMinimal(getString("RiakPlugin.Port.Label")   + " : '" + port   + "'");
        logMinimal(getString("RiakPlugin.Bucket.Label") + " : '" + bucket + "'");
        logMinimal(getString("RiakPlugin.Value.Label")  + " : '" + value  + "'");
        logMinimal(getString("RiakPlugin.Key.Label")    + " : '" + key    + "'");

        if (host == null) {
            throw new RiakPluginException("Invalid riak host name : " + host);
        }

        if (port == null) {
            throw new RiakPluginException("Invalid riak port : " + port);
        }

        if (bucket == null) {
            throw new RiakPluginException("Invalid bucket name : " + bucket);
        }

        if (key == null) {
            throw new RiakPluginException("Invalid key field : " + key);
        }

        if (mode == RiakPluginData.Mode.PUT && value == null) {
            throw new RiakPluginException("Invalid value field : " + value);
        }

        // get field index
        data.valueFieldIndex = data.outputRowMeta.indexOfValue(value);
        data.keyFieldIndex   = data.outputRowMeta.indexOfValue(key);
        data.port            = Integer.valueOf(port);
        data.resolver        = resolver;
        data.vclock          = vclock;
        data.bucket          = bucket;
        data.host            = host;
        data.mode            = mode;

        if (data.vclock != null) {
            data.vclockFieldIndex = data.outputRowMeta.indexOfValue(data.vclock);

            if (data.vclockFieldIndex < 0) {
                throw new RiakPluginException("Unable to retrieve vclock field : " + vclock);
            }
        }

        if (mode == RiakPluginData.Mode.PUT && data.valueFieldIndex < 0) {
            throw new RiakPluginException("Unable to retrieve value field : " + value);
        }

        if (data.keyFieldIndex < 0) {
            throw new RiakPluginException("Unable to retrieve key field : " + key);
        }

        processor = factory.processorFor(this, data, meta);

    }

    @Override
    public void dispose(StepMetaInterface smi, StepDataInterface sdi)
    {
        meta = (RiakPluginMeta) smi;
        data = (RiakPluginData) sdi;

        processor.shutdown();
        super.dispose(smi, sdi);
    }
}
