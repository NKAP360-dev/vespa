// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.handler;

import com.google.inject.Inject;
import com.yahoo.container.core.LogserverConfig;
import com.yahoo.container.jdisc.HttpRequest;
import com.yahoo.container.jdisc.HttpResponse;
import com.yahoo.container.jdisc.ThreadedHttpRequestHandler;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Executor;

public class LogHandler extends ThreadedHttpRequestHandler {

    private final LogReader logReader;

    @Inject
    public LogHandler(Executor executor, LogserverConfig config) {
        this(executor, new LogReader(config.logDirectory(), config.logPattern()));
    }

    LogHandler(Executor executor, LogReader logReader) {
        super(executor);
        this.logReader = logReader;
    }

    @Override
    public HttpResponse handle(HttpRequest request) {
        JSONObject responseJSON = new JSONObject();

        HashMap<String, String> apiParams = getParameters(request);
        long earliestLogThreshold = getEarliestThreshold(apiParams);
        long latestLogThreshold = getLatestThreshold(apiParams);
        try {
            JSONObject logJson = logReader.readLogs(earliestLogThreshold, latestLogThreshold);
            responseJSON.put("logs", logJson);
        } catch (IOException | JSONException e) {
            return new HttpResponse(404) {
                @Override
                public void render(OutputStream outputStream) {}
            };
        }
        return new HttpResponse(200) {
            @Override
            public void render(OutputStream outputStream) throws IOException {
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
                outputStreamWriter.write(responseJSON.toString());
                outputStreamWriter.close();
            }
        };
    }

    private HashMap<String, String> getParameters(HttpRequest request) {
        String query = request.getUri().getQuery();
        HashMap<String, String> keyValPair = new HashMap<>();
        Arrays.stream(query.split("&")).forEach(pair -> {
            String[] splitPair = pair.split("=");
            keyValPair.put(splitPair[0], splitPair[1]);
        });
        return keyValPair;
    }

    private long getEarliestThreshold(HashMap<String, String> map) {
        if (map.containsKey("from")) {
            return Long.valueOf(map.get("from"));
        }
        return Long.MIN_VALUE;
    }

    private long getLatestThreshold(HashMap<String, String> map) {
        if (map.containsKey("to")) {
            return Long.valueOf(map.get("to"));
        }
        return Long.MAX_VALUE;
    }
}
