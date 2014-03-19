
package com.instaclick.pentaho.plugin.riak;

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
import org.w3c.dom.Node;

public class RiakPluginMeta extends BaseStepMeta implements StepMetaInterface
{
    private static final String FIELD_HOST     = "host";
    private static final String FIELD_PORT     = "port";
    private static final String FIELD_BUCKET   = "bucket";
    private static final String FIELD_RESOLVER = "resolver";
    private static final String FIELD_VALUE    = "value";
    private static final String FIELD_KEY      = "key";
    private static final String FIELD_MODE     = "mode";

    private RiakPluginData.Mode mode = RiakPluginData.Mode.GET;
    private String resolver;
    private String bucket;
    private String host;
    private String port;
    private String value;
    private String key;

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
    public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space) throws KettleStepException
    {
        if (RiakPluginData.Mode.GET != mode) {
            return;
        }

        // a value meta object contains the meta data for a field
        final ValueMetaInterface b = new ValueMeta(getValue(), ValueMeta.TYPE_STRING);
        // the name of the step that adds this field
        b.setOrigin(name);
        // modify the row structure and add the field this step generates
        inputRowMeta.addValueMeta(b);
    }

    @Override
    public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev, String[] input, String[] output, RowMetaInterface info)
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

        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_MODE, getMode().toString()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_RESOLVER, getResolver()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_BUCKET, getBucket()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_VALUE, getValue()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_HOST, getHost()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_PORT, getPort()));
        bufer.append("   ").append(XMLHandler.addTagValue(FIELD_KEY, getKey()));

        return bufer.toString();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleXMLException
    {
        try {
            setResolver(XMLHandler.getTagValue(stepnode, FIELD_RESOLVER));
            setBucket(XMLHandler.getTagValue(stepnode, FIELD_BUCKET));
            setValue(XMLHandler.getTagValue(stepnode, FIELD_VALUE));
            setMode(XMLHandler.getTagValue(stepnode, FIELD_MODE));
            setHost(XMLHandler.getTagValue(stepnode, FIELD_HOST));
            setPort(XMLHandler.getTagValue(stepnode, FIELD_PORT));
            setKey(XMLHandler.getTagValue(stepnode, FIELD_KEY));

        } catch (Exception e) {
            throw new KettleXMLException("Unable to read step info from XML node", e);
        }
    }

    @Override
    public void readRep(Repository rep, ObjectId idStep, List<DatabaseMeta> databases, Map<String, Counter> counters) throws KettleException
    {
        try {
            setResolver(rep.getStepAttributeString(idStep, FIELD_RESOLVER));
            setBucket(rep.getStepAttributeString(idStep, FIELD_BUCKET));
            setValue(rep.getStepAttributeString(idStep, FIELD_VALUE));
            setMode(rep.getStepAttributeString(idStep, FIELD_MODE));
            setHost(rep.getStepAttributeString(idStep, FIELD_HOST));
            setPort(rep.getStepAttributeString(idStep, FIELD_PORT));
            setKey(rep.getStepAttributeString(idStep, FIELD_KEY));

        } catch (KettleDatabaseException dbe) {
            throw new KettleException("error reading step with id_step=" + idStep + " from the repository", dbe);
        } catch (KettleException e) {
            throw new KettleException("Unexpected error reading step with id_step=" + idStep + " from the repository", e);
        }
    }

    @Override
    public void saveRep(Repository rep, ObjectId idTransformation, ObjectId idStep) throws KettleException
    {
        try {
            rep.saveStepAttribute(idTransformation, idStep, FIELD_MODE, getMode().toString());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_RESOLVER, getResolver());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_BUCKET, getBucket());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_VALUE, getValue());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_HOST, getHost());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_PORT, getPort());
            rep.saveStepAttribute(idTransformation, idStep, FIELD_KEY, getKey());

        } catch (KettleDatabaseException dbe) {
            throw new KettleException("Unable to save step information to the repository, id_step=" + idStep, dbe);
        }
    }

    @Override
    public void setDefault()
    {
        this.mode = RiakPluginData.Mode.GET;
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

    public String getResolver()
    {
        return resolver;
    }

    public void setResolver(String resolver)
    {
        this.resolver = resolver;
    }

    public void setMode(RiakPluginData.Mode mode)
    {
        this.mode = mode;
    }

    public void setMode(String mode)
    {
        this.mode = RiakPluginData.Mode.valueOf(mode);
    }

    public String getBucket()
    {
        return bucket;
    }

    public void setBucket(String bucket)
    {
        this.bucket = bucket;
    }

    public String getHost()
    {
        return host;
    }

    public void setHost(String host)
    {
        this.host = host;
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
}
