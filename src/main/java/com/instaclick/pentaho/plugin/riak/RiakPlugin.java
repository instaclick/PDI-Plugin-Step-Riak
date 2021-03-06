package com.instaclick.pentaho.plugin.riak;

import com.basho.riak.client.core.query.Namespace;
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
import com.instaclick.pentaho.plugin.riak.RiakPluginMeta.SecondaryIndex;
import com.instaclick.pentaho.plugin.riak.processor.Processor;
import com.instaclick.pentaho.plugin.riak.processor.ProcessorFactory;
import java.io.IOException;
import org.pentaho.di.core.Const;

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
    public boolean processRow(final StepMetaInterface smi, final StepDataInterface sdi) throws KettleException, KettleStepException
    {
        meta       = (RiakPluginMeta) smi;
        data       = (RiakPluginData) sdi;
        Object[] r = getRow();

        if (first) {
            first = false;

            if (r == null) {
                setOutputDone();
                return false;
            }

            try {
                initPlugin();
            } catch (final IOException e) {
                throw new RiakPluginException(e.getMessage(), e);
            }
        }

        if (r == null) {
            setOutputDone();

            return false;
        }

        // log progress
        if (checkFeedback(getLinesWritten())) {
            logBasic(String.format("liner %s", getLinesWritten()));
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
        meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);

        final String contentType        = environmentSubstitute(meta.getContentType());
        final String bucketType         = environmentSubstitute(meta.getBucketType());
        final String resolver           = environmentSubstitute(meta.getResolver());
        final String bucket             = environmentSubstitute(meta.getBucket());
        final String vclock             = environmentSubstitute(meta.getVClock());
        final String uri                = environmentSubstitute(meta.getUri());
        final String value              = environmentSubstitute(meta.getValue());
        final String key                = environmentSubstitute(meta.getKey());
        final RiakPluginData.Mode mode  = meta.getMode();

        logMinimal(getString("RiakPlugin.Mode.Label")       + " : '" + mode   + "'");
        logMinimal(getString("RiakPlugin.Uri.Label")        + " : '" + uri   + "'");
        logMinimal(getString("RiakPlugin.Bucket.Label")     + " : '" + bucket + "'");
        logMinimal(getString("RiakPlugin.BucketType.Label") + " : '" + bucketType + "'");
        logMinimal(getString("RiakPlugin.Value.Label")      + " : '" + value  + "'");
        logMinimal(getString("RiakPlugin.Key.Label")        + " : '" + key    + "'");

        if (uri == null) {
            throw new RiakPluginException("Invalid riak uri : " + uri);
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
        data.bucketType      = bucketType;
        data.resolver        = resolver;
        data.vclock          = vclock;
        data.bucket          = bucket;
        data.uri             = uri;
        data.mode            = mode;
        data.bucketType      = Const.isEmpty(bucketType) 
            ? Namespace.DEFAULT_BUCKET_TYPE
            : bucketType;

        if (mode != RiakPluginData.Mode.DELETE && ! Const.isEmpty(data.vclock)) {
            data.vclockFieldIndex = data.outputRowMeta.indexOfValue(data.vclock);

            if (data.vclockFieldIndex < 0) {
                throw new RiakPluginException("Unable to retrieve vclock field : " + vclock);
            }
        }

        if ( ! Const.isEmpty(contentType)) {
            data.contentTypeFieldIndex = data.outputRowMeta.indexOfValue(contentType);

            if (data.contentTypeFieldIndex < 0 && mode == RiakPluginData.Mode.PUT) {
                throw new RiakPluginException("Unable to retrieve Content-Type field : " + contentType);
            }
        }

        for (final SecondaryIndex i2 : meta.getSecondaryIndexes()) {
            final String indexType   = environmentSubstitute(i2.getType());
            final String fieldName   = environmentSubstitute(i2.getField());
            final String indexName   = environmentSubstitute(i2.getIndex());
            final Integer fieldIndex = data.outputRowMeta.indexOfValue(fieldName);

            if (mode == RiakPluginData.Mode.PUT && fieldIndex < 0) {
                throw new RiakPluginException("Unable to retrieve index field : " + fieldName);
            }

            if ( ! "int".endsWith(indexType) && ! "bin".endsWith(indexType)) {
                throw new RiakPluginException("Unknown index type : " + indexType);
            }

            data.indexes.add(new RiakPluginData.Index(indexName, fieldIndex, indexType));
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

        if (processor != null) {
            processor.shutdown();
        }

        super.dispose(smi, sdi);
    }
}
