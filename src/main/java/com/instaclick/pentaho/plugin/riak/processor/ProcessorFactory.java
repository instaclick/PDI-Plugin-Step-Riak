package com.instaclick.pentaho.plugin.riak.processor;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.google.common.base.Splitter;
import com.instaclick.pentaho.plugin.riak.RiakPlugin;
import com.instaclick.pentaho.plugin.riak.RiakPluginData;
import com.instaclick.pentaho.plugin.riak.RiakPluginMeta;
import java.io.IOException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessorFactory
{
    private Map<String, String> parseUriParameters(final String query)
    {
        if (query == null || "".equals(query)) {
            return new HashMap<String, String>(0);
        }

        return Splitter.on("&")
            .withKeyValueSeparator("=")
            .split(query);
    }

    private int getParameter(final Map<String,String> params, final String name, final int defaultValue)
    {
        if ( ! params.containsKey(name)) {
            return defaultValue;
        }

        return Integer.parseInt(params.get(name));
    }

    private List<RiakNode> createNodes(final RiakPluginData data) throws UnknownHostException
    {
        final List<RiakNode> nodes = new ArrayList<RiakNode>();
        final String[] hosts       = data.uri.split(",");

        for (String host : hosts) {
            if (host == null || "".equals(host)) {
                throw new NullPointerException("Invalid connection uri : " + data.uri);
            }

            if ( ! host.toLowerCase().startsWith("proto://")) {
                host = "proto://" + host;
            }

            final URI uri                   = URI.create(host);
            final Map<String,String> params = this.parseUriParameters(uri.getQuery());
            final String address            = uri.getHost() != null ? uri.getHost() : host;
            final int port                  = uri.getPort() > 0 ? uri.getPort() : RiakNode.Builder.DEFAULT_REMOTE_PORT;
            final int connectionTimeout     = getParameter(params, "connectionTimeout", RiakNode.Builder.DEFAULT_CONNECTION_TIMEOUT);
            final int minConnections        = getParameter(params, "minConnections", RiakNode.Builder.DEFAULT_MIN_CONNECTIONS);
            final int maxConnections        = getParameter(params, "maxConnections", RiakNode.Builder.DEFAULT_MIN_CONNECTIONS);
            final int idleTimeoutInMillis   = getParameter(params, "idleTimeout", RiakNode.Builder.DEFAULT_IDLE_TIMEOUT);
            final RiakNode.Builder builder  = new RiakNode.Builder()
                    .withConnectionTimeout(connectionTimeout)
                    .withIdleTimeout(idleTimeoutInMillis)
                    .withMaxConnections(maxConnections)
                    .withMinConnections(minConnections)
                    .withRemoteAddress(address)
                    .withRemotePort(port);

            nodes.add(builder.build());
        }

        return nodes;
    }

    private RiakClient clientFor(final RiakPluginData data) throws IOException
    {
        final List<RiakNode> nodes = this.createNodes(data);
        final RiakCluster cluster  = RiakCluster.builder(nodes).build();

        cluster.start();

        return new RiakClient(cluster);
    }

    public Processor processorFor(final RiakPlugin step,final RiakPluginData data, final RiakPluginMeta meta) throws IOException
    {
        final RiakClient client = clientFor(data);

        if (RiakPluginData.Mode.GET == data.mode) {
            return new GetProcessor(client, step, data);
        }

        if (RiakPluginData.Mode.PUT == data.mode) {
            return new PutProcessor(client, step, data);
        }

        if (RiakPluginData.Mode.DELETE == data.mode) {
            return new DeleteProcessor(client, step, data);
        }

        throw new RuntimeException("Unknown mode : " + data.mode);
    }
}
