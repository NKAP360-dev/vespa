// Copyright 2018 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.container.handler;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Base64;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

class LogReader {

    private final Path logDirectory;
    private final Pattern logFilePattern;

    LogReader(String logDirectory, String logFilePattern) {
        this(Paths.get(logDirectory), Pattern.compile(logFilePattern));
    }

    private LogReader(Path logDirectory, Pattern logFilePattern) {
        this.logDirectory = logDirectory;
        this.logFilePattern = logFilePattern;
    }

    JSONObject readLogs(long earliestLogThreshold, long latestLogThreshold) throws IOException, JSONException {
        JSONObject json = new JSONObject();
        latestLogThreshold = latestLogThreshold + Duration.ofMinutes(5).toMillis(); // Add some time to allow retrieving logs currently being modified
        traverse_folder(logDirectory, json, earliestLogThreshold, latestLogThreshold, "");
        return json;
    }

    private void traverse_folder(Path path, JSONObject json, long earliestLogThreshold, long latestLogThreshold, String filenamePrefix) throws IOException, JSONException {
        try (Stream<Path> files = Files.list(path)) {
            for (Iterator<Path> it = files.iterator(); it.hasNext(); ) {
                Path child = it.next();
                String filename = child.getFileName().toString();
                long logTime = Files.getLastModifiedTime(child).toMillis();
                if (Files.isRegularFile(child)) {
                    if (earliestLogThreshold < logTime && logTime < latestLogThreshold && logFilePattern.matcher(filename).matches()) {
                        json.put(filenamePrefix + filename, Base64.getEncoder().encodeToString(Files.readAllBytes(child)));
                    }
                } else {
                    traverse_folder(child, json, earliestLogThreshold, latestLogThreshold, filenamePrefix + filename + "-");
                }
            }
        }
    }

}
