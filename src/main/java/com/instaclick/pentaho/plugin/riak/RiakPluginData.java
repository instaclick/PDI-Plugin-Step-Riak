package com.instaclick.pentaho.plugin.riak;

import java.util.ArrayList;
import java.util.List;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;

public class RiakPluginData extends BaseStepData implements StepDataInterface
{
    public List<Index> indexes = new ArrayList<Index>();
    public RowMetaInterface outputRowMeta;
    public Integer contentTypeFieldIndex;
    public Integer vclockFieldIndex;
    public Integer valueFieldIndex;
    public Integer keyFieldIndex;
    public String bucketType;
    public String resolver;
    public String bucket;
    public String vclock;
    public String uri;
    public Mode mode;

    public static class Index
    {
        final public Integer field;
        final public String index;
        final public String type;

        public Index(final String index, final Integer field, final String type)
        {
            this.index = index;
            this.field = field;
            this.type  = type;
        }
    }

    public enum Mode
    {
        GET,
        PUT,
        DELETE
    }
}
