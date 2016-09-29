# Graylog WebHDFS Output Plugin

An output plugin to write messages to Hadoop HDFS over WebHDFS.

Tested with Hadoop 2.7.0.

The WebHDFS implementation uses modified code from [webhdfs-java-client](https://github.com/zxs/webhdfs-java-client).

Getting started
---------------

To start using this plugin place this [jar] (https://github.com/sivasamyk/graylog-plugin-output-webhdfs/releases/download/1.0.1/graylog-plugin-output-webhdfs-1.0.1.jar) in the plugins directory and restart graylog server. 

Following parameters can be configured while launching the plugin

* Host - IP Address or hostname of the HDFS name node
* Port - WebHDFS port configured in HDFS. 
* Username - Username of pseudo authentication (currently kerberos is not supported)
* File path - Path of file to store the messages. File name can be formatted with message fields or date formats. E.g ${source}_%Y_%m_%d.log for storing the messages based source and day.
* Message Format - Format of message to be written. Can be formatted with message fields like ${timestamp} | ${source} | ${short_message}
* Flush interval - Interval in seconds to flush the data to HDFS. Value of 0 means immediate.

![Plugin configuration window] (https://github.com/sivasamyk/graylog-plugin-output-webhdfs/raw/master/webhdfs-plugin-config.png)

