package com.instaclick.pentaho.plugin.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.cap.ConflictResolver;
import java.util.Collection;

public class AggregateSiblingsResolver implements ConflictResolver<IRiakObject>
{
    Collection<IRiakObject> siblings;

    public Collection<IRiakObject> getSiblings()
    {
        return siblings;
    }

    public boolean hasSiblings()
    {
        return siblings != null && siblings.size() > 1;
    }

    @Override
    public IRiakObject resolve(Collection<IRiakObject> siblings)
    {
        this.siblings = siblings;

        if (siblings.size() == 1) {
            return siblings.iterator().next();
        }

        return null;
    }
}
