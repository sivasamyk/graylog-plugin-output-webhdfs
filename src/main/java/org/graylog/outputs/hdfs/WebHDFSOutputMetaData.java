package org.graylog.outputs.hdfs;

import org.graylog2.plugin.PluginMetaData;
import org.graylog2.plugin.ServerStatus;
import org.graylog2.plugin.Version;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * Implement the PluginMetaData interface here.
 */
public class WebHDFSOutputMetaData implements PluginMetaData {
    @Override
    public String getUniqueId() {
        return "org.graylog.outputs.hdfs.WebHDFSOutputPlugin";
    }

    @Override
    public String getName() {
        return "WebHDFSOutput";
    }

    @Override
    public String getAuthor() {
        return "Sivasamy Kaliappan";
    }

    @Override
    public URI getURL() {
        return URI.create("https://www.graylog.org/");
    }

    @Override
    public Version getVersion() {
        return new Version(1, 0, 1);
    }

    @Override
    public String getDescription() {
        return "Forwards the output to Hadoop Distributed File System (HDFS) using WebHDFS";
    }

    @Override
    public Version getRequiredVersion() {
        return new Version(1, 0, 0);
    }

    @Override
    public Set<ServerStatus.Capability> getRequiredCapabilities() {
        return Collections.emptySet();
    }
}
