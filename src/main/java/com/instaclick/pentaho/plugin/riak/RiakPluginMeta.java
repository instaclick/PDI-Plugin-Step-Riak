
package com.instaclick.pentaho.plugin.riak;

import com.basho.riak.client.core.query.Namespace;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.core.Const;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

public class RiakPluginMeta extends BaseStepMeta implements StepMetaInterface
{
    private static final String FIELD_BUCKET_TYPE   = "bucket_type";
    private static final String FIELD_CONTENT_TYPE  = "content_type";
    private static final String FIELD_HOST          = "host";
    private static final String FIELD_PORT          = "port";
    private static final String FIELD_BUCKET        = "bucket";
    private static final String FIELD_RESOLVER      = "resolver";
    private static final String FIELD_VCLOCK        = "vclock";
    private static final String FIELD_VALUE         = "value";
    private static final String FIELD_KEY           = "key";
    private static final String FIELD_MODE          = "mode";

    private final List<SecondaryIndex> secondaryIndexes = new ArrayList<SecondaryIndex>();
    private RiakPluginData.Mode mode = RiakPluginData.Mode.GET;
    private String bucketType = Namespace.DEFAULT_BUCKET_TYPE;
    private String contentType;
    private String resolver;
    private String bucket;
    private String uri;
    private String port;
    private String value;
    private String vclock;
    private String key;

    public static class SecondaryIndex
    {
        private final String index;
        private final String field;
        private final String type;

        SecondaryIndex(final String index, final String field, final String type)
        {
            this.index = index;
            this.field = field;
            this.type  = type;
        }

        public String getIndex()
        {
            return index;
        }

        public String getField()
        {
            return field;
        }

        public String getType()
        {
            return type;
        }
    }

    public RiakPluginMeta() {
        super();
    }

    public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name)
    {
        return new RiakPluginDialog(shell, meta, transMeta, name);
    }

    @Override
    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta, Trans disp)
    {
        return new RiakPlugin(stepMeta, stepDataInterface, cnr, transMeta, disp);
    }

    @Override
    public StepDataInterface getStepData()
    {
        return new RiakPluginData();
    }

    @Override
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space, Repository repository, IMetaStore ims) throws KettleStepException
    {
        if (RiakPluginData.Mode.GET != mode) {
            return;
        }

        final ValueMetaInterface valueField = new ValueMeta(getValue(), ValueMeta.TYPE_STRING);

        valueField.setOrigin(name);
        inputRowMeta.addValueMeta(valueField);

        if ( ! Const.isEmpty(vclock)) {
            final ValueMetaInterface vclockField = new ValueMeta(vclock, ValueMeta.TYPE_BINARY);
            valueField.setOrigin(name);
            inputRowMeta.addValueMeta(vclockField);
        }

        if ( ! Const.isEmpty(contentType)) {
            final ValueMetaInterface contentTypeField = new ValueMeta(contentType, ValueMeta.TYPE_STRING);
            valueField.setOrigin(name);
            inputRowMeta.addValueMeta(contentTypeField);
        }
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info, VariableSpace vs, Repository rpstr, IMetaStore ims)
    {
        final CheckResult prevSizeCheck = (prev == null || prev.isEmpty())
            ? new CheckResult(CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!", stepMeta)
            : new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is connected to previous one, receiving " + prev.size() + " fields", stepMeta);

        /// See if we have input streams leading to this step!
        final CheckResult inputLengthCheck = (input.length > 0)
            ? new CheckResult(CheckResult.TYPE_RESULT_OK, "Step is receiving info from other steps.", stepMeta)
            : new CheckResult(CheckResult.TYPE_RESULT_ERROR, "No input received from other steps!", stepMeta);

        remarks.add(prevSizeCheck);
        remarks.add(inputLengthCheck);
    }

    @Override
    public String getXML()
    {
        final StringBuilder bufer = new StringBuilder();

        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_CONTENT_TYPE, getContentType()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_BUCKET_TYPE, getBucketType()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_MODE, getMode().toString()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_RESOLVER, getResolver()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_BUCKET, getBucket()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_VCLOCK, getVClock()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_VALUE, getValue()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_HOST, getUri()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_PORT, getPort()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_KEY, getKey()));

        return bufer.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore ims) throws KettleXMLException
    {
        try {
            setContentType(XMLHandler.getTagValue(stepnode, FIELD_CONTENT_TYPE));
            setBucketType(XMLHandler.getTagValue(stepnode, FIELD_BUCKET_TYPE));
            setResolver(XMLHandler.getTagValue(stepnode, FIELD_RESOLVER));
            setBucket(XMLHandler.getTagValue(stepnode, FIELD_BUCKET));
            setVClock(XMLHandler.getTagValue(stepnode, FIELD_VCLOCK));
            setValue(XMLHandler.getTagValue(stepnode, FIELD_VALUE));
            setMode(XMLHandler.getTagValue(stepnode, FIELD_MODE));
            setUri(XMLHandler.getTagValue(stepnode, FIELD_HOST));
            setPort(XMLHandler.getTagValue(stepnode, FIELD_PORT));
            setKey(XMLHandler.getTagValue(stepnode, FIELD_KEY));

        } catch (Exception e) {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    @Override
    public void readRep(Repository rep, IMetaStore ims, ObjectId idStep, List<DatabaseMeta> databases) throws KettleException
    {
        try {
            setContentType(rep.getStepAttributeString(idStep, FIELD_CONTENT_TYPE));
            setBucketType(rep.getStepAttributeString(idStep, FIELD_BUCKET_TYPE));
            setResolver(rep.getStepAttributeString(idStep, FIELD_RESOLVER));
            setBucket(rep.getStepAttributeString(idStep, FIELD_BUCKET));
            setVClock(rep.getStepAttributeString(idStep, FIELD_VCLOCK));
            setValue(rep.getStepAttributeString(idStep, FIELD_VALUE));
            setMode(rep.getStepAttributeString(idStep, FIELD_MODE));
            setUri(rep.getStepAttributeString(idStep, FIELD_HOST));
            setPort(rep.getStepAttributeString(idStep, FIELD_PORT));
            setKey(rep.getStepAttributeString(idStep, FIELD_KEY));

        } catch (KettleDatabaseException dbe) {
            throw new KettleException("error reading step with id_step=" + idStep + " from the repository", dbe);
        } catch (KettleException e) {
            throw new KettleException("Unexpected error reading step with id_step=" + idStep + " from the repository", e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore ims, ObjectId idTransformation, ObjectId idStep) throws KettleException
    {
        try {
            rep.saveStepAttribute(idTransformation, idStep, FIELD_CONTENT_TYPE, getContentType());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_BUCKET_TYPE, getBucketType());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_MODE, getMode().toString());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_RESOLVER, getResolver());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_BUCKET, getBucket());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_VCLOCK, getVClock());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_VALUE, getValue());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_HOST, getUri());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_PORT, getPort());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_KEY, getKey());

        } catch (KettleDatabaseException dbe) {
            throw new KettleException("Unable to save step information to the repository, id_step=" + idStep, dbe);
        }
    }

    @Override
    public void setDefault()
    {
        this.mode       = RiakPluginData.Mode.GET;
        this.bucketType = Namespace.DEFAULT_BUCKET_TYPE; 
    }

    @Override
    public boolean supportsErrorHandling() 
    {
        return false;
    }

    public RiakPluginData.Mode getMode()
    {
        if (this.mode == null) {
            this.mode = RiakPluginData.Mode.GET;
        }

        return mode;
    }

    public void setMode(RiakPluginData.Mode mode)
    {
        this.mode = mode;
    }

    public void setMode(String mode)
    {
        this.mode = RiakPluginData.Mode.valueOf(mode);
    }

    public String getResolver()
    {
        return resolver;
    }

    public void setResolver(String resolver)
    {
        this.resolver = resolver;
    }

    public String getVClock()
    {
        return vclock;
    }

    public void setVClock(String vclock)
    {
        this.vclock = vclock;
    }

    public String getBucket()
    {
        return bucket;
    }

    public void setBucket(String bucket)
    {
        this.bucket = bucket;
    }
    
    public String getBucketType()
    {
        return bucketType;
    }

    public void setBucketType(String type)
    {
        if (Const.isEmpty(type)) {
            type = Namespace.DEFAULT_BUCKET_TYPE; 
        }

        this.bucketType = type;
    }

    public String getContentType()
    {
        return contentType;
    }

    public void setContentType(String type)
    {
        this.contentType = type;
    }

    public String getUri()
    {
        return uri;
    }

    public void setUri(String uri)
    {
        this.uri = uri;
    }

    public String getPort()
    {
        return port;
    }

    public void setPort(String port)
    {
        this.port = port;
    }

    public String getValue()
    {
        return value;
    }

    public void setValue(String body)
    {
        this.value = body;
    }

    public String getKey()
    {
        return key;
    }

    public void setKey(String key)
    {
        this.key = key;
    }

    public void addSecondaryIndex(final String index, final String field, final String type)
    {
        this.secondaryIndexes.add(new SecondaryIndex(index, field, type));
    }

    public List<SecondaryIndex> getSecondaryIndexes()
    {
        return this.secondaryIndexes;
    }

    public void clearSecondaryIndex()
    {
        this.secondaryIndexes.clear();
    }
}
