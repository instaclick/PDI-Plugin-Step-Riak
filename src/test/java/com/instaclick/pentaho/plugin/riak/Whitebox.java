package com.instaclick.pentaho.plugin.riak;

import com.basho.riak.client.api.commands.kv.FetchValue;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Thanks to "final class" and "protected abstract class" !!
 */
public class Whitebox
{
    public static Field getResponseField(final String fieldName) throws NoSuchFieldException
    {
        for (final Field superClassFild : FetchValue.Response.class.getSuperclass().getDeclaredFields()) {
            if ( ! superClassFild.getName().equals(fieldName)) {
                continue;
            }

            return superClassFild;
        }

        return FetchValue.Response.class.getDeclaredField(fieldName);
    }

    public static void seResponsetFieldValue(final FetchValue.Response response, final String fieldName, final Object value) throws NoSuchFieldException, IllegalArgumentException, IllegalAccessException
    {
        final Field field     = getResponseField(fieldName);
        final Field modifiers = Field.class.getDeclaredField("modifiers");

        modifiers.setAccessible(true);
        modifiers.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.setAccessible(true);
        field.set(response, value);
    }
}
