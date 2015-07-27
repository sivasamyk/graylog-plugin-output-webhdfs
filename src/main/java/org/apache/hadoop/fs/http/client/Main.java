package org.apache.hadoop.fs.http.client;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Created on 5/7/15.
 */
public class Main {
    public static void main(String args[]) throws  Exception
    {
        WebHDFSConnection connection =
                new WebHDFSConnection("http://localhost:50070","hadoopuser","anything", AuthenticationType.PSEUDO);
        //System.out.println(connection.listStatus("user/hadoopuser"));
        System.out.print(connection.getHomeDirectory());
        ByteArrayInputStream stream = new ByteArrayInputStream("India is my Country".getBytes());
        System.out.println(connection.append("tmp/india2.txt", stream));
        ByteArrayOutputStream os  = new ByteArrayOutputStream();
        System.out.println(connection.open("tmp/india2.txt",os));
        System.out.println(new String(os.toByteArray()));
    }
}
