// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.search.dispatch;

import com.yahoo.compress.CompressionType;
import com.yahoo.compress.Compressor;
import com.yahoo.document.GlobalId;
import com.yahoo.document.idstring.IdIdString;
import com.yahoo.prelude.fastsearch.FastHit;
import com.yahoo.search.Result;
import com.yahoo.slime.ArrayTraverser;
import com.yahoo.slime.BinaryFormat;
import com.yahoo.slime.Cursor;
import com.yahoo.slime.Inspector;
import com.yahoo.slime.Slime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author bratseth
 */
public class MockClient implements Client {

    private final Map<DocsumKey, Map<String, Object>> docsums = new HashMap<>();
    private final Compressor compressor = new Compressor();
    private boolean malfunctioning = false;
    private Result searchResult;

    /** Set to true to cause this to produce an error instead of a regular response */
    public void setMalfunctioning(boolean malfunctioning) { this.malfunctioning = malfunctioning; }

    @Override
    public NodeConnection createConnection(String hostname, int port) {
        return new MockNodeConnection(hostname, port);
    }

    @Override
    public void getDocsums(List<FastHit> hitsContext, NodeConnection node, CompressionType compression,
                           int uncompressedSize, byte[] compressedSlime, RpcFillInvoker.GetDocsumsResponseReceiver responseReceiver,
                           double timeoutSeconds) {
        if (malfunctioning) {
            responseReceiver.receive(GetDocsumsResponseOrError.fromError("Malfunctioning"));
            return;
        }

        Inspector request = BinaryFormat.decode(compressor.decompress(compressedSlime, compression, uncompressedSize)).get();
        String docsumClass = request.field("class").asString();
        List<Map<String, Object>> docsumsToReturn = new ArrayList<>();
        request.field("gids").traverse((ArrayTraverser)(index, gid) -> {
            GlobalId docId = new GlobalId(gid.asData());
            docsumsToReturn.add(docsums.get(new DocsumKey(node.toString(), docId, docsumClass)));
        });
        Slime responseSlime = new Slime();
        Cursor root = responseSlime.setObject();
        Cursor docsums = root.setArray("docsums");
        for (Map<String, Object> docsumFields : docsumsToReturn) {
            Cursor docsumItem = docsums.addObject();
            Cursor docsum = docsumItem.setObject("docsum");
            for (Map.Entry<String, Object> field : docsumFields.entrySet()) {
                if (field.getValue() instanceof Integer)
                    docsum.setLong(field.getKey(), (Integer)field.getValue());
                else if (field.getValue() instanceof String)
                    docsum.setString(field.getKey(), (String)field.getValue());
                else
                    throw new RuntimeException();
            }
        }
        byte[] slimeBytes = BinaryFormat.encode(responseSlime);
        Compressor.Compression compressionResult = compressor.compress(compression, slimeBytes);
        GetDocsumsResponse response = new GetDocsumsResponse(compressionResult.type().getCode(), slimeBytes.length,
                                                             compressionResult.data(), hitsContext);
        responseReceiver.receive(GetDocsumsResponseOrError.fromResponse(response));
    }

    @Override
    public void search(NodeConnection node, CompressionType compression, int uncompressedLength, byte[] compressedPayload,
            RpcSearchInvoker responseReceiver, double timeoutSeconds) {
        if (malfunctioning) {
            responseReceiver.receive(SearchResponseOrError.fromError("Malfunctioning"));
            return;
        }

        if(searchResult == null) {
            responseReceiver.receive(SearchResponseOrError.fromError("No result defined"));
            return;
        }
        var payload = searchResult.toProtobuf().toByteArray();
        var compressionResult = compressor.compress(compression, payload);
        var response = new SearchResponse(compressionResult.type().getCode(), payload.length, compressionResult.data());
        responseReceiver.receive(SearchResponseOrError.fromResponse(response));
    }

    public void setDocsumReponse(String nodeId, int docId, String docsumClass, Map<String, Object> docsumValues) {
        docsums.put(new DocsumKey(nodeId, globalIdFrom(docId), docsumClass), docsumValues);
    }

    public GlobalId globalIdFrom(int hitId) {
        return new GlobalId(new IdIdString("", "test", "", String.valueOf(hitId)));
    }

    private static class MockNodeConnection implements Client.NodeConnection {

        private final String hostname;

        public MockNodeConnection(String hostname, int port) {
            this.hostname = hostname;
        }

        @Override
        public void close() { }

        @Override
        public String toString() { return hostname; }

    }

    private static class DocsumKey {

        private final String internalKey;

        public DocsumKey(String nodeId, GlobalId docId, String docsumClass) {
            internalKey = docsumClass + "." + nodeId + "." + docId;
        }

        @Override
        public int hashCode() { return internalKey.hashCode(); }

        @Override
        public boolean equals(Object other) {
            if ( ! (other instanceof DocsumKey)) return false;
            return ((DocsumKey)other).internalKey.equals(this.internalKey);
        }

        @Override
        public String toString() { return internalKey; }

    }

}
