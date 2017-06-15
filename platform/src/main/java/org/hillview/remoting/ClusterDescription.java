package org.hillview.remoting;

import com.google.common.net.HostAndPort;

import java.util.List;

/**
 * Describes the list of hosts that comprise a cluster
 */
public final class ClusterDescription {
    private final List<HostAndPort> serverList;

    public ClusterDescription(final List<HostAndPort> serverList) {
        this.serverList = serverList;
    }

    public List<HostAndPort> getServerList() {
        return this.serverList;
    }
}