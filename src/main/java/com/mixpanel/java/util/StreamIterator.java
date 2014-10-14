package com.mixpanel.java.util;

/**
 ** File:    StreamIterator.java
 ** Author:  Christian Roche <christian.roche@workshare.com>
 ** Purpose: Implements Iterator interface to MixPanel export request.
 ** Date:    2014/09/30
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;

import com.mixpanel.java.MPException;
import com.sun.org.apache.xpath.internal.functions.FuncFalse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class StreamIterator implements Iterator<String> {

    // Provide buffered input from URI.
    private BufferedReader br;

    // Store next line in input stream.
    private String nextLine;

    public StreamIterator(final URI uri) throws MPException {

        try {
            HttpClient httpclient = new DefaultHttpClient();
            HttpGet httpGet = new HttpGet(uri);
            HttpResponse response = httpclient.execute(httpGet);
            InputStream content = response.getEntity().getContent();

            br = new BufferedReader(new InputStreamReader(content));

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new MPException(StringUtils.inputStreamToString(content));
            }

            if (hasNext() && nextLine.contains("'error':")) {
                JSONObject responseJson = new JSONObject(StringUtils.inputStreamToString(content));
                throw new MPException(responseJson.getString("error"), responseJson.getString("request"));
            }

        } catch (Throwable e) {
            throw new MPException(e);
        }
    }

    @Override
    public boolean hasNext() {

        if (nextLine == null)
            try {
                nextLine = br.readLine();
            } catch (IOException e) {
                nextLine = null;
            }

        return (nextLine != null);
    }

    @Override
    public String next() {


        if (nextLine == null)
            try {
                nextLine = br.readLine();
            } catch (IOException e) {
                nextLine = null;
            }

        String ret = nextLine;
        nextLine = null;
        return ret;
    }

    @Override
    public void remove() {

    }
}
