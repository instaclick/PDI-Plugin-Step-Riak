package com.instaclick.pentaho.plugin.riak;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class RiakPluginData extends BaseStepData implements StepDataInterface
{
    public RowMetaInterface outputRowMeta;
    public Integer vclockFieldIndex;
    public Integer valueFieldIndex;
    public Integer keyFieldIndex;
    public String resolver;
    public String bucket;
    public String vclock;
    public String host;
    public Integer port;
    public Mode mode;

    public enum Mode
    {
        GET,
        PUT,
        DELETE
    }
}
