package com.instaclick.pentaho.plugin.riak;

public class RiakPluginException extends RuntimeException
{
    public RiakPluginException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public RiakPluginException(String message)
    {
        super(message);
    }
}
