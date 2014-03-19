package com.instaclick.pentaho.plugin.riak;

public class RiakException extends RuntimeException
{
    public RiakException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public RiakException(String message)
    {
        super(message);
    }
}
