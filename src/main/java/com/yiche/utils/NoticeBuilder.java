package com.yiche.utils;


import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class NoticeBuilder {

    private static final String SCHEME = "http";

    //private static final String HOST = "172.20.4.127:9820";


    private static final String HOST = "yu.yiche.com";


    private static final String SERVICE_VERSION = "/api/v1";
    private static final String SERVICE_NAME = "/notice";

    private static final String SERVICE_PATH = "/send";

    private static final String CONTENT_TYPE = "application/json";
    private static final String CONTENT_ENCODE = "utf-8";

    private static final String DEFAULT_GROUP_UNIQUE_ID = "00000000";

    private static final int BUFFER_SIZE = 10240;

    //private static final String HTTP_GET = "GET";
    private static final String HTTP_PUT = "PUT";
    private static final String HTTP_POST = "POST";
    //private static final String HTTP_DELETE = "DELETE";

    private Map<String, Object> params;
    private Gson gson;

    private String host;
    private String path;

    public enum Operation {
        SEND(HTTP_POST);

        private String httpMethod;

        Operation(String httpMethod) {
            this.httpMethod = httpMethod;
        }

        public String getMethod() {
            return httpMethod;
        }

    }

    private NoticeBuilder(String path) {
        this.gson = new Gson();
        this.params = new HashMap<>();
        this.path = path;
        this.host = HOST;
    }

    /**
     * Convenience method that creates a <code>HttpURLConnection</code>.
     * <p/>
     * This methods performs and injects any needed authentication credentials
     * via the {@link #getConnection(URL, String)} method
     *
     * @param method        the HTTP method.
     * @param params        the query string parameters.
     * @param path          the file path
     * @param makeQualified if the path should be 'makeQualified'
     * @return a <code>HttpURLConnection</code> for the HttpFSServer server,
     * authenticated and ready to use for the specified path and file
     * system operation.
     * @throws IOException thrown if an IO error occurrs.
     */
    private HttpURLConnection getConnection(String host, String method, String path,
                                            Map<String, String> params) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append(SCHEME).append("://").append(host)
                .append(SERVICE_VERSION)
                .append(SERVICE_NAME)
                .append(path);

        if ((params != null) && (params.size() > 0)) {
            String separator = "?";
            for (Map.Entry<String, String> entry : params.entrySet()) {
                sb.append(separator).append(entry.getKey()).append("=")
                        .append(URLEncoder.encode(entry.getValue(), "UTF8"));
                separator = "&";
            }
        }
        URL url = new URL(sb.toString());
        return getConnection(url, method);
    }

    private HttpURLConnection getConnection(URL url, String method)
            throws IOException {
        try {
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod(method);
            if (method.equals(HTTP_POST) || method.equals(HTTP_PUT)) {
                conn.setDoOutput(true);
            }
            return conn;
        } catch (Exception ex) {
            throw new IOException(ex);
        }
    }

    /**
     * Validates the status of an <code>HttpURLConnection</code> against an
     * expected HTTP status code. If the current status code is not the expected
     * one it throws an exception with a detail message using Server side error
     * messages if available.
     *
     * @param conn     the <code>HttpURLConnection</code>.
     * @param expected the expected HTTP status code.
     * @throws IOException thrown if the current status code does not match the expected
     *                     one.
     */
    private boolean validateResponse(HttpURLConnection conn, int expected)
            throws IOException {
        int status = conn.getResponseCode();
        if (status == expected) {
            try {
                Map<String, String> result = jsonParse(conn);
                String code = result.get("code");
                //String message = result.get("msg");
                if ("0".equals(code)) {
                    return true;
                } else {
                    return false;
                }
            } catch (IOException ex) {
                if (ex.getCause() instanceof IOException) {
                    throw (IOException) ex.getCause();
                }
                throw new IOException(MessageFormat.format(
                        "HTTP status [{0}], {1}", status,
                        conn.getResponseMessage()));
            }
        }
        return false;
    }

    /**
     * Convenience method that JSON Parses the <code>InputStream</code> of a
     * <code>HttpURLConnection</code>.
     *
     * @param conn the <code>HttpURLConnection</code>.
     * @return the parsed JSON object.
     * @throws IOException thrown if the <code>InputStream</code> could not be JSON
     *                     parsed.
     */
    private Map<String, String> jsonParse(HttpURLConnection conn) throws IOException {
        java.lang.reflect.Type type =
                new TypeToken<HashMap<String, String>>() {
                }.getType();
        return gson.fromJson(new InputStreamReader(conn.getInputStream()), type);
    }

    /*
     * Common handling for uploading data for create and append operations.
     */
    private boolean send(String host, String method, String path, String content,
                         Map<String, String> params, int bufferSize, int expectedStatus)
            throws IOException {
        HttpURLConnection conn = getConnection(host, method, path, params);
        try {
            conn.setRequestProperty("Content-Type", CONTENT_TYPE);
            conn.setRequestProperty("Accept-Charset", CONTENT_ENCODE);
            try (OutputStream os = new BufferedOutputStream(conn.getOutputStream(), bufferSize)) {
                os.write(content.getBytes(CONTENT_ENCODE));
                os.flush();
                return validateResponse(conn, expectedStatus);
            } catch (IOException ex) {
                throw ex;
            }
        } catch (IOException ex) {
            throw ex;
        }
    }

    private static final String GROUP_UNIQUE_ID = "groupUniqueId";
    private static final String EMAIL_SENDER = "emailSender";
    private static final String EMAIL_SENDER_ALIAS = "emailSenderAlias";
    private static final String EMAIL_SUBJECT = "emailSubject";
    private static final String EMAIL_TOLIST = "emailToList";
    private static final String EMAIL_CC = "emailCc";
    private static final String EMAIL_BCC = "emailBcc";
    private static final String EMAIL_ATTACHMENT = "emailAttachment";
    private static final String WX_TO_LIST = "wxToList";
    private static final String DATA_CONTENT = "dataContent";

    public static NoticeBuilder createNoticeSend() {
        return new NoticeBuilder(SERVICE_PATH);
    }

    public static NoticeBuilder createNoticeSendWithDefaultGroup() {
        NoticeBuilder builder = new NoticeBuilder(SERVICE_PATH);
        builder.setGroupUniqueId(DEFAULT_GROUP_UNIQUE_ID);
        return builder;
    }

    public NoticeBuilder setHost(String host) {
        this.host = host;
        return this;
    }

    public NoticeBuilder setServicePath(String path) {
        this.path = path;
        return this;
    }

    public NoticeBuilder setGroupUniqueId(String groupUniqueId) {
        params.put(GROUP_UNIQUE_ID, groupUniqueId);
        return this;
    }

    public NoticeBuilder setEmailSender(String emailSender) {
        params.put(EMAIL_SENDER, emailSender);
        return this;
    }

    public NoticeBuilder setEmailSenderAlias(String emailSenderAlias) {
        params.put(EMAIL_SENDER_ALIAS, emailSenderAlias);
        return this;
    }

    public NoticeBuilder setEmailSubject(String emailSubject) {
        params.put(EMAIL_SUBJECT, emailSubject);
        return this;
    }

    public NoticeBuilder setEmailToList(String emailToList) {
        params.put(EMAIL_TOLIST, emailToList);
        return this;
    }

    public NoticeBuilder setEmailCc(String emailCc) {
        params.put(EMAIL_CC, emailCc);
        return this;
    }

    public NoticeBuilder setEmailBcc(String emailBcc) {
        params.put(EMAIL_BCC, emailBcc);
        return this;
    }

    public NoticeBuilder setEmailAttachment(String emailAttachment) {
        params.put(EMAIL_ATTACHMENT, emailAttachment);
        return this;
    }

    public NoticeBuilder setWxToList(String wxToList) {
        params.put(WX_TO_LIST, wxToList);
        return this;
    }

    public NoticeBuilder setDataContent(String dataContent) {
        params.put(DATA_CONTENT, dataContent);
        return this;
    }

    public boolean sendNotice() throws IOException {
        String content = gson.toJson(params);
        System.out.println("content" + content);
//        return true;
        return send(
                host,
                Operation.SEND.getMethod(),
                path,
                content,
                null,
                BUFFER_SIZE,
                HttpURLConnection.HTTP_OK);
    }
}
