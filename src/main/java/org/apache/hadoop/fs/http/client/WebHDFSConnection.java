package org.apache.hadoop.fs.http.client;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Date;

/**
 * ===== HTTP GET <br/>
 * <li>OPEN (see FileSystem.open)
 * <li>GETFILESTATUS (see FileSystem.getFileStatus)
 * <li>LISTSTATUS (see FileSystem.listStatus)
 * <li>GETCONTENTSUMMARY (see FileSystem.getContentSummary)
 * <li>GETFILECHECKSUM (see FileSystem.getFileChecksum)
 * <li>GETHOMEDIRECTORY (see FileSystem.getHomeDirectory)
 * <li>GETDELEGATIONTOKEN (see FileSystem.getDelegationToken)
 * <li>GETDELEGATIONTOKENS (see FileSystem.getDelegationTokens)
 * <br/>
 * ===== HTTP PUT <br/>
 * <li>CREATE (see FileSystem.create)
 * <li>MKDIRS (see FileSystem.mkdirs)
 * <li>CREATESYMLINK (see FileContext.createSymlink)
 * <li>RENAME (see FileSystem.rename)
 * <li>SETREPLICATION (see FileSystem.setReplication)
 * <li>SETOWNER (see FileSystem.setOwner)
 * <li>SETPERMISSION (see FileSystem.setPermission)
 * <li>SETTIMES (see FileSystem.setTimes)
 * <li>RENEWDELEGATIONTOKEN (see FileSystem.renewDelegationToken)
 * <li>CANCELDELEGATIONTOKEN (see FileSystem.cancelDelegationToken)
 * <br/>
 * ===== HTTP POST <br/>
 * APPEND (see FileSystem.append)
 * <br/>
 * ===== HTTP DELETE <br/>
 * DELETE (see FileSystem.delete)
 */
public class WebHDFSConnection {

    protected static final Logger logger = LoggerFactory.getLogger(WebHDFSConnection.class);

    private String httpfsUrl = null;
    private String principal;
    private String password;

    private Token token = new AuthenticatedURL.Token();
    private AuthenticatedURL authenticatedURL;
    private AuthenticationType authenticationType;

    public WebHDFSConnection(String httpfsUrl, String principal, String password,
                             AuthenticationType authenticationType) {
        this.httpfsUrl = httpfsUrl;
        this.principal = principal;
        this.password = password;
        this.authenticationType = authenticationType;
        if (this.authenticationType == AuthenticationType.PSEUDO) {
            this.authenticatedURL = new AuthenticatedURL(new PseudoAuthenticator2(principal));
        } else {
            this.authenticatedURL = new AuthenticatedURL(new KerberosAuthenticator2(principal, password));
        }
    }


    protected HttpURLConnection getURLConnection(String uri) throws AuthenticationException,IOException {
        HttpURLConnection conn;
        if (authenticationType == AuthenticationType.KERBEROS) {
            conn = authenticatedURL.openConnection(
                    new URL(new URL(httpfsUrl), uri), token);
        } else {
            String spec = uri + "&user.name=" + principal;
            conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        }
        return conn;
    }

    public static synchronized Token generateToken(String srvUrl, String princ, String passwd,
                                                   AuthenticationType authenticationType) {
        AuthenticatedURL.Token newToken = new AuthenticatedURL.Token();

        Authenticator authenticator = null;
        String spec;
        if (authenticationType == AuthenticationType.KERBEROS) {
            authenticator = new KerberosAuthenticator2(princ,passwd);
            spec = "/webhdfs/v1/?op=GETHOMEDIRECTORY";
        } else {
            authenticator = new PseudoAuthenticator2(princ);
            spec = MessageFormat.format("/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name={0}", princ);
        }
        try {
            HttpURLConnection conn = new AuthenticatedURL(authenticator).openConnection(new URL(new URL(srvUrl), spec), newToken);
            conn.connect();
            conn.disconnect();
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            logger.error("[" + princ + ":" + passwd + "]@" + srvUrl, ex);
            // WARN
            // throws  IOException,
            // AuthenticationException, InterruptedException
        }

        return newToken;
    }

    protected static long copy(InputStream input, OutputStream result) throws IOException {
        byte[] buffer = new byte[12288]; // 8K=8192 12K=12288 64K=
        long count = 0L;
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            result.write(buffer, 0, n);
            count += n;
            result.flush();
        }
        result.flush();
        return count;
    }

    private static Response result(HttpURLConnection conn, boolean input) throws IOException {
        StringBuffer sb = new StringBuffer();
        if (input) {
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String line = null;

            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            reader.close();
            is.close();
        }

        Response response = new Response();
        response.setCode(conn.getResponseCode());
        response.setStatus(conn.getResponseMessage());
        response.setContentType(conn.getContentType());
        response.setData(sb.toString());
        return response;
    }

    public void ensureValidToken() {
        if (!token.isSet()) { // if token is null
            token = generateToken(httpfsUrl, principal, password,authenticationType);
        } else {
            long currentTime = new Date().getTime();
            long tokenExpired = Long.parseLong(token.toString().split("&")[3].split("=")[1]);
            logger.debug("[currentTime vs. tokenExpired] " + currentTime + " " + tokenExpired);

            if (currentTime > tokenExpired) { // if the token is expired
                token = generateToken(httpfsUrl, principal, password,authenticationType);
            }
        }

    }

	/*
     * ========================================================================
	 * GET
	 * ========================================================================
	 */

    /**
     * <b>GETHOMEDIRECTORY</b>
     * <p/>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/?op=GETHOMEDIRECTORY"
     *
     * @throws MalformedURLException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getHomeDirectory() throws IOException, AuthenticationException {
        ensureValidToken();
        //String spec = MessageFormat.format("/webhdfs/v1/?op=GETHOMEDIRECTORY&user.name={0}", this.principal);
        //HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection("/webhdfs/v1/?op=GETHOMEDIRECTORY");
        conn.connect();
        Response response = result(conn, true);
        conn.disconnect();
        return response.getData();
    }

    /**
     * <b>OPEN</b>
     * <p/>
     * curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN
     * [&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
     *
     * @param path
     * @param os
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String open(String path, OutputStream os) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=OPEN&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=OPEN", URLUtil.encodePath(path)));
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.connect();
        InputStream is = conn.getInputStream();
        copy(is, os);
        is.close();
        os.close();
        Response resp = result(conn, false);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>GETCONTENTSUMMARY</b>
     * <p/>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETCONTENTSUMMARY"
     *
     * @param path
     * @throws MalformedURLException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getContentSummary(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=GETCONTENTSUMMARY&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=GETCONTENTSUMMARY", URLUtil.encodePath(path)));
        conn.setRequestMethod("GET");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>LISTSTATUS</b>
     * <p/>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
     *
     * @param path
    
     * @throws MalformedURLException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String listStatus(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=LISTSTATUS&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=LISTSTATUS", URLUtil.encodePath(path)));
        conn.setRequestMethod("GET");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>GETFILESTATUS</b>
     * <p/>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
     *
     * @param path
     * @throws MalformedURLException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getFileStatus(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=GETFILESTATUS&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=LISTSTATUS", URLUtil.encodePath(path)));
        conn.setRequestMethod("GET");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>GETFILECHECKSUM</b>
     * <p/>
     * curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILECHECKSUM"
     *
     * @param path
     * @throws MalformedURLException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String getFileCheckSum(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=GETFILECHECKSUM&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=GETFILECHECKSUM", URLUtil.encodePath(path)));
        conn.setRequestMethod("GET");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

	/*
	 * ========================================================================
	 * PUT
	 * ========================================================================
	 */

    /**
     * <b>CREATE</b>
     * <p/>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CREATE
     * [&overwrite=<true|false>][&blocksize=<LONG>][&replication=<SHORT>]
     * [&permission=<OCTAL>][&buffersize=<INT>]"
     *
     * @param path
     * @param is
    
     * @throws MalformedURLException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String create(String path, InputStream is) throws IOException,
            AuthenticationException {
        ensureValidToken();
        //String spec = MessageFormat.format("/webhdfs/v1/{0}?op=CREATE&user.name={1}", URLUtil.encodePath(path), this.principal);

        String redirectUrl = null;

        //HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=CREATE", URLUtil.encodePath(path)));
        conn.setRequestMethod("PUT");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        logger.debug("Location:" + conn.getHeaderField("Location"));
        Response resp = result(conn, true);
        if (conn.getResponseCode() == 307)
            redirectUrl = conn.getHeaderField("Location");
        conn.disconnect();

        if (redirectUrl != null) {
            if(authenticationType == AuthenticationType.KERBEROS) {
                conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
            } else {
                conn = (HttpURLConnection) new URL(redirectUrl).openConnection();
            }
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            // conn.setRequestProperty("Transfer-Encoding", "chunked");
            final int _SIZE = is.available();
            conn.setRequestProperty("Content-Length", "" + _SIZE);
            conn.setFixedLengthStreamingMode(_SIZE);
            conn.connect();
            OutputStream os = conn.getOutputStream();
            copy(is, os);
            // Util.copyStream(is, os);
            is.close();
            os.close();
            resp = result(conn, false);
            conn.disconnect();
        }

        return resp.getData();
    }

    /**
     * <b>MKDIRS</b>
     * <p/>
     * curl -i -X PUT
     * "http://<HOST>:<PORT>/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
     *
     * @param path
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String mkdirs(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=MKDIRS&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=MKDIRS", URLUtil.encodePath(path)));
        conn.setRequestMethod("PUT");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>CREATESYMLINK</b>
     * <p/>
     * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=CREATESYMLINK
     * &destination=<PATH>[&createParent=<true|false>]"
     *
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String createSymLink(String srcPath, String destPath) throws IOException,
            AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=CREATESYMLINK&destination={1}&user.name={2}",
//                URLUtil.encodePath(srcPath), URLUtil.encodePath(destPath), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=CREATESYMLINK&destination={1}",
                URLUtil.encodePath(srcPath),URLUtil.encodePath(destPath)));
        conn.setRequestMethod("PUT");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>RENAME</b>
     * <p/>
     * curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=RENAME
     * &destination=<PATH>[&createParent=<true|false>]"
     *
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String rename(String srcPath, String destPath) throws IOException,
            AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=RENAME&destination={1}&user.name={2}",
//                URLUtil.encodePath(srcPath), URLUtil.encodePath(destPath), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=RENAME&destination={1}",
                URLUtil.encodePath(srcPath),URLUtil.encodePath(destPath)));
        conn.setRequestMethod("PUT");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>SETPERMISSION</b>
     * <p/>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETPERMISSION
     * [&permission=<OCTAL>]"
     *
     * @param path
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String setPermission(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETPERMISSION&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=SETPERMISSION", URLUtil.encodePath(path)));
        conn.setRequestMethod("PUT");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>SETOWNER</b>
     * <p/>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETOWNER
     * [&owner=<USER>][&group=<GROUP>]"
     *
     * @param path
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String setOwner(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETOWNER&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=SETOWNER", URLUtil.encodePath(path)));
        conn.setRequestMethod("PUT");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>SETREPLICATION</b>
     * <p/>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETREPLICATION
     * [&replication=<SHORT>]"
     *
     * @param path
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String setReplication(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETREPLICATION&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=SETREPLICATION", URLUtil.encodePath(path)));
        conn.setRequestMethod("PUT");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    /**
     * <b>SETTIMES</b>
     * <p/>
     * curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=SETTIMES
     * [&modificationtime=<TIME>][&accesstime=<TIME>]"
     *
     * @param path
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String setTimes(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=SETTIMES&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=SETTIMES", URLUtil.encodePath(path)));
        conn.setRequestMethod("PUT");
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

	/*
	 * ========================================================================
	 * POST
	 * ========================================================================
	 */

    /**
     * curl -i -X POST
     * "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
     *
     * @param path
     * @param is
    
     * @throws MalformedURLException
     * @throws IOException
     * @throws AuthenticationException
     */
    public String append(String path, InputStream is) throws IOException,
            AuthenticationException {
        ensureValidToken();
        //String spec = MessageFormat.format("/webhdfs/v1/{0}?op=APPEND&user.name={1}", URLUtil.encodePath(path), this.principal);
        String redirectUrl = null;
       // HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=APPEND", URLUtil.encodePath(path)));
        conn.setRequestMethod("POST");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        logger.debug("Location:" + conn.getHeaderField("Location"));
        Response resp = result(conn, true);
        if (conn.getResponseCode() == 307)
            redirectUrl = conn.getHeaderField("Location");
        conn.disconnect();

        if (redirectUrl != null) {
            if(authenticationType == AuthenticationType.KERBEROS) {
                conn = authenticatedURL.openConnection(new URL(redirectUrl), token);
            } else {
                conn = (HttpURLConnection) new URL(redirectUrl).openConnection();
            }
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            conn.setRequestProperty("Content-Type", "application/octet-stream");
            // conn.setRequestProperty("Transfer-Encoding", "chunked");
            final int _SIZE = is.available();
            conn.setRequestProperty("Content-Length", "" + _SIZE);
            conn.setFixedLengthStreamingMode(_SIZE);
            conn.connect();
            OutputStream os = conn.getOutputStream();
            copy(is, os);
            // Util.copyStream(is, os);
            is.close();
            os.close();
            resp = result(conn, true);
            conn.disconnect();
        }

        return resp.getData();
    }

	/*
	 * ========================================================================
	 * DELETE
	 * ========================================================================
	 */

    /**
     * <b>DELETE</b>
     * <p/>
     * curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE
     * [&recursive=<true|false>]"
     *
     * @param path
    
     * @throws AuthenticationException
     * @throws IOException
     * @throws MalformedURLException
     */
    public String delete(String path) throws IOException, AuthenticationException {
        ensureValidToken();
//        String spec = MessageFormat.format("/webhdfs/v1/{0}?op=DELETE&user.name={1}", URLUtil.encodePath(path), this.principal);
//        HttpURLConnection conn = authenticatedURL.openConnection(new URL(new URL(httpfsUrl), spec), token);
        HttpURLConnection conn = getURLConnection(MessageFormat.format("/webhdfs/v1/{0}?op=DELETE", URLUtil.encodePath(path)));
        conn.setRequestMethod("DELETE");
        conn.setInstanceFollowRedirects(false);
        conn.connect();
        Response resp = result(conn, true);
        conn.disconnect();

        return resp.getData();
    }

    // Begin Getter & Setter
    public String getHttpfsUrl() {
        return httpfsUrl;
    }

    public void setHttpfsUrl(String httpfsUrl) {
        this.httpfsUrl = httpfsUrl;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    // End Getter & Setter
}
