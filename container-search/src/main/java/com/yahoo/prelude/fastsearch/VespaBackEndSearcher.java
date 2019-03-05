// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.prelude.fastsearch;

import com.yahoo.collections.TinyIdentitySet;
import com.yahoo.fs4.DocsumPacket;
import com.yahoo.fs4.DocumentInfo;
import com.yahoo.fs4.Packet;
import com.yahoo.fs4.QueryPacket;
import com.yahoo.fs4.QueryPacketData;
import com.yahoo.fs4.QueryResultPacket;
import com.yahoo.io.GrowableByteBuffer;
import com.yahoo.io.HexDump;
import com.yahoo.log.LogLevel;
import com.yahoo.prelude.ConfigurationException;
import com.yahoo.prelude.query.Item;
import com.yahoo.prelude.query.NullItem;
import com.yahoo.prelude.query.textualrepresentation.TextualQueryRepresentation;
import com.yahoo.prelude.querytransform.QueryRewrite;
import com.yahoo.processing.request.CompoundName;
import com.yahoo.protect.Validator;
import com.yahoo.search.Query;
import com.yahoo.search.Result;
import com.yahoo.search.cluster.PingableSearcher;
import com.yahoo.search.grouping.vespa.GroupingExecutor;
import com.yahoo.search.result.Coverage;
import com.yahoo.search.result.ErrorHit;
import com.yahoo.search.result.ErrorMessage;
import com.yahoo.search.result.Hit;
import com.yahoo.search.result.Relevance;
import com.yahoo.search.searchchain.Execution;
import com.yahoo.searchlib.aggregation.Grouping;
import com.yahoo.vespa.objects.BufferSerializer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Superclass for backend searchers.
 *
 * @author  baldersheim
 */
@SuppressWarnings("deprecation")
public abstract class VespaBackEndSearcher extends PingableSearcher {

    static final CompoundName PACKET_COMPRESSION_LIMIT = new CompoundName("packetcompressionlimit");
    static final CompoundName PACKET_COMPRESSION_TYPE = new CompoundName("packetcompressiontype");
    private static final CompoundName TRACE_DISABLE = new CompoundName("trace.disable");

    private String serverId;

    /** The set of all document databases available in the backend handled by this searcher */
    private Map<String, DocumentDatabase> documentDbs = new LinkedHashMap<>();
    private DocumentDatabase defaultDocumentDb = null;

    /** Default docsum class. null means "unset" and is the default value */
    private String defaultDocsumClass = null;

    /** Returns an iterator which returns all hits below this result **/
    static Iterator<Hit> hitIterator(Result result) {
        return result.hits().unorderedDeepIterator();
    }

    /** The name of this source */
    private String name;

    /** Cache wrapper */
    private CacheControl cacheControl = null;

    public final String getName() { return name; }
    protected final String getDefaultDocsumClass() { return defaultDocsumClass; }

    /** Sets default document summary class. Default is null */
    private void setDefaultDocsumClass(String docsumClass) { defaultDocsumClass = docsumClass; }

    /** Returns the packet cache controller of this */
    public final CacheControl getCacheControl() { return cacheControl; }

    public final Logger getLogger() { return super.getLogger(); }

    /**
     * Searches a search cluster
     * This is an endpoint - searchers will never propagate the search to any nested searcher.
     *
     * @param query the query to search
     * @param queryPacket the serialized query representation to pass to the search cluster
     * @param cacheKey the cache key created from the query packet, or null if caching is not used
     * @param execution the query execution context
     */
    protected abstract Result doSearch2(Query query, QueryPacket queryPacket, CacheKey cacheKey, Execution execution);

    protected abstract void doPartialFill(Result result, String summaryClass);

    /**
     * Returns whether we need to send the query when fetching summaries.
     * This is necessary if the query requests summary features or dynamic snippeting
     */
    public boolean summaryNeedsQuery(Query query) {
        if (query.getRanking().getQueryCache()) return false;  // Query is cached in backend

        DocumentDatabase documentDb = getDocumentDatabase(query);

        // Needed to generate a dynamic summary?
        DocsumDefinition docsumDefinition = documentDb.getDocsumDefinitionSet().getDocsum(query.getPresentation().getSummary());
        if (docsumDefinition.isDynamic()) return true;

        // Needed to generate ranking features?
        RankProfile rankProfile = documentDb.rankProfiles().get(query.getRanking().getProfile());
        if (rankProfile == null) return true; // stay safe
        if (rankProfile.hasSummaryFeatures()) return true;
        if (query.getRanking().getListFeatures()) return true;

        // (Don't just add other checks here as there is a return false above)

        return false;
    }

    private Result cacheLookupFirstPhase(CacheKey key, QueryPacketData queryPacketData, Query query, int offset, int hits, String summaryClass) {
        PacketWrapper packetWrapper = cacheControl.lookup(key, query);

        if (packetWrapper == null) return null;

        // Check if the cache entry contains the requested hits
        List<DocumentInfo> documents = packetWrapper.getDocuments(offset, hits);
        if (documents == null) return null;

        if (query.getPresentation().getSummary() == null)
            query.getPresentation().setSummary(getDefaultDocsumClass());
        Result result = new Result(query);
        QueryResultPacket resultPacket = packetWrapper.getFirstResultPacket();

        addMetaInfo(query, queryPacketData, resultPacket, result);
        if (packetWrapper.getNumPackets() == 0)
            addUnfilledHits(result, documents, true, queryPacketData, key, packetWrapper.distributionKey());
        else
            addCachedHits(result, packetWrapper, summaryClass, documents);
        return result;
    }

    public String getServerId() { return serverId; }

    public DocumentDatabase getDocumentDatabase(Query query) {
        if (query.getModel().getRestrict().size() == 1) {
            String docTypeName = (String)query.getModel().getRestrict().toArray()[0];
            DocumentDatabase db = documentDbs.get(docTypeName);
            if (db != null) {
                return db;
            }
        }
        return defaultDocumentDb;
    }

    private void resolveDocumentDatabase(Query query) {
        DocumentDatabase docDb = getDocumentDatabase(query);
        if (docDb != null) {
            query.getModel().setDocumentDb(docDb.getName());
        }
    }

    public final void init(String serverId, SummaryParameters docSumParams, ClusterParams clusterParams,
                           CacheParams cacheParams, DocumentdbInfoConfig documentdbInfoConfig) {
        this.serverId = serverId;
        this.name = clusterParams.searcherName;

        Validator.ensureNotNull("Name of Vespa backend integration", getName());

        setDefaultDocsumClass(docSumParams.defaultClass);

        if (documentdbInfoConfig != null) {
            for (DocumentdbInfoConfig.Documentdb docDb : documentdbInfoConfig.documentdb()) {
                DocumentDatabase db = new DocumentDatabase(docDb);
                if (documentDbs.isEmpty()) {
                    defaultDocumentDb = db;
                }
                documentDbs.put(docDb.name(), db);
            }
        }

        if (cacheParams.cacheControl == null) {
            this.cacheControl = new CacheControl(cacheParams.cacheMegaBytes, cacheParams.cacheTimeOutSeconds);
        } else {
            this.cacheControl = cacheParams.cacheControl;
        }
    }

    protected void transformQuery(Query query) { }

    public Result search(Query query, Execution execution) {
        // query root should not be null here
        Item root = query.getModel().getQueryTree().getRoot();
        if (root == null || root instanceof NullItem) {
            return new Result(query, ErrorMessage.createNullQuery(query.getHttpRequest().getUri().toString()));
        }

        QueryRewrite.optimizeByRestrict(query);
        QueryRewrite.optimizeAndNot(query);
        QueryRewrite.collapseSingleComposites(query);

        root = query.getModel().getQueryTree().getRoot();
        if (root == null || root instanceof NullItem) // root can become null after optimization
            return new Result(query);

        resolveDocumentDatabase(query);
        transformQuery(query);
        traceQuery(name, "search", query, query.getOffset(), query.getHits(), 1, Optional.empty());

        root = query.getModel().getQueryTree().getRoot();
        if (root == null || root instanceof NullItem) // root can become null after resolving and transformation?
            return new Result(query);

        QueryPacket queryPacket = createQueryPacket(serverId, query);

        if (isLoggingFine())
            getLogger().fine("made QueryPacket: " + queryPacket);

        Result result = null;
        CacheKey cacheKey = null;
        if (cacheControl.useCache(query)) {
            cacheKey = new CacheKey(queryPacket);
            result = getCached(cacheKey, queryPacket.getQueryPacketData(), query);
        }

        if (result == null) {
            result = doSearch2(query, queryPacket, cacheKey, execution);
            if (isLoggingFine())
                getLogger().fine("Result NOT retrieved from cache");

            if (query.getTraceLevel() >= 1)
                query.trace(getName() + " dispatch response: " + result, false, 1);
            result.trace(getName());
        }
        return result;
    }

    protected QueryPacket createQueryPacket(String serverId, Query query) {
        QueryPacket queryPacket = QueryPacket.create(serverId, query);
        int compressionLimit = query.properties().getInteger(PACKET_COMPRESSION_LIMIT, 0);
        queryPacket.setCompressionLimit(compressionLimit);
        if (compressionLimit != 0)
            queryPacket.setCompressionType(query.properties().getString(PACKET_COMPRESSION_TYPE, "lz4"));
        return queryPacket;
    }

    /**
     * Returns a cached result, or null if no result was cached for this key
     *
     * @param cacheKey the cache key created from the query packet
     * @param queryPacketData a serialization of the query, to avoid having to recompute this, or null if not available
     * @param query the query, used for tracing, lookup of result window and result creation
     */
    private Result getCached(CacheKey cacheKey, QueryPacketData queryPacketData, Query query) {
        if (query.getTraceLevel() >= 6) {
            query.trace("Cache key hash: " + cacheKey.hashCode(), 6);
            if (query.getTraceLevel() >= 8) {
                query.trace("Cache key: " + HexDump.toHexString(cacheKey.getCopyOfFullKey()), 8);
            }
        }

        Result result = cacheLookupFirstPhase(cacheKey, queryPacketData, query, query.getOffset(), query.getHits(), query.getPresentation().getSummary());
        if (result == null) return null;

        if (isLoggingFine()) {
            getLogger().fine("Result retrieved from cache: " + result);
        }
        if (query.getTraceLevel() >= 1) {
            query.trace(getName() + " cached response: " + result, false, 1);
        }
        result.trace(getName());
        return result;
    }

    private List<Result> partitionHits(Result result, String summaryClass) {
        List<Result> parts = new ArrayList<>();
        TinyIdentitySet<Query> queryMap = new TinyIdentitySet<>(4);

        for (Iterator<Hit> i = hitIterator(result); i.hasNext(); ) {
            Hit hit = i.next();
            if (hit instanceof FastHit) {
                FastHit fastHit = (FastHit) hit;
                if ( ! fastHit.isFilled(summaryClass)) {
                    Query q = fastHit.getQuery();
                    if (q == null) {
                        q = result.hits().getQuery(); // fallback for untagged hits
                    }
                    int idx = queryMap.indexOf(q);
                    if (idx < 0) {
                        idx = queryMap.size();
                        Result r = new Result(q);
                        parts.add(r);
                        queryMap.add(q);
                    }
                    parts.get(idx).hits().add(fastHit);
                }
            }
        }
        return parts;
    }

    @Override
    public void fill(Result result, String summaryClass, Execution execution) {
        if (result.isFilled(summaryClass)) return; // TODO: Checked in the superclass - remove

        List<Result> parts= partitionHits(result, summaryClass);
        if (parts.size() > 0) { // anything to fill at all?
            for (Result r : parts) {
                doPartialFill(r, summaryClass);
                mergeErrorsInto(result, r);
            }
            result.hits().setSorted(false);
            result.analyzeHits();
        }
    }

    private void mergeErrorsInto(Result destination, Result source) {
        ErrorHit eh = source.hits().getErrorHit();
        if (eh != null) {
            for (ErrorMessage error : eh.errors())
                destination.hits().addError(error);
        }
    }

    static void traceQuery(String sourceName, String type, Query query, int offset, int hits, int level, Optional<String> quotedSummaryClass) {
        if ((query.getTraceLevel()<level) || query.properties().getBoolean(TRACE_DISABLE)) return;

        StringBuilder s = new StringBuilder();
        s.append(sourceName).append(" " + type + " to dispatch: ")
                .append("query=[")
                .append(query.getModel().getQueryTree().getRoot().toString())
                .append("]");

        s.append(" timeout=").append(query.getTimeout()).append("ms");

        s.append(" offset=")
                .append(offset)
                .append(" hits=")
                .append(hits);

        if (query.getRanking().hasRankProfile()) {
            s.append(" rankprofile[")
                .append(query.getRanking().getProfile())
                .append("]");
        }

        if (query.getRanking().getFreshness() != null) {
            s.append(" freshness=")
                    .append(query.getRanking().getFreshness().getRefTime());
        }

        if (query.getRanking().getSorting() != null) {
            s.append(" sortspec=")
                    .append(query.getRanking().getSorting().fieldOrders().toString());
        }

        if (query.getRanking().getLocation() != null) {
            s.append(" location=")
                    .append(query.getRanking().getLocation().toString());
        }

        if (query.getGroupingSessionCache()) {
            s.append(" groupingSessionCache=true");
        }
        if (query.getRanking().getQueryCache()) {
            s.append(" ranking.queryCache=true");
        }
        if (query.getGroupingSessionCache() || query.getRanking().getQueryCache()) {
            s.append(" sessionId=").append(query.getSessionId());
        }

        List<Grouping> grouping = GroupingExecutor.getGroupingList(query);
        s.append(" grouping=").append(grouping.size()).append(" : ");
        for(Grouping g : grouping) {
            s.append(g.toString());
        }

        if ( ! query.getRanking().getProperties().isEmpty()) {
            s.append(" rankproperties=")
                    .append(query.getRanking().getProperties().toString());
        }

        if ( ! query.getRanking().getFeatures().isEmpty()) {
            s.append(" rankfeatures=")
                    .append(query.getRanking().getFeatures().toString());
        }

        if (query.getModel().getRestrict() != null) {
            s.append(" restrict=").append(query.getModel().getRestrict().toString());
        }

        if (quotedSummaryClass.isPresent()) {
            s.append(" summary=").append(quotedSummaryClass.get());
        }

        query.trace(s.toString(), false, level);
        if (query.isTraceable(level + 1)) {
            query.trace("Current state of query tree: "
                            + new TextualQueryRepresentation(query.getModel().getQueryTree().getRoot()),
                    false, level+1);
        }
        if (query.isTraceable(level + 2)) {
            query.trace("YQL+ representation: " + query.yqlRepresentation(), level+2);
        }
    }

    void addMetaInfo(Query query, QueryPacketData queryPacketData, QueryResultPacket resultPacket, Result result) {
        result.setTotalHitCount(resultPacket.getTotalDocumentCount());

        // Grouping
        if (resultPacket.getGroupData() != null) {
            byte[] data = resultPacket.getGroupData();
            ArrayList<Grouping> list = new ArrayList<>();
            BufferSerializer buf = new BufferSerializer(new GrowableByteBuffer(ByteBuffer.wrap(data)));
            int cnt = buf.getInt(null);
            for (int i = 0; i < cnt; i++) {
                Grouping g = new Grouping();
                g.deserialize(buf);
                list.add(g);
            }
            GroupingListHit hit = new GroupingListHit(list, getDocsumDefinitionSet(query));
            hit.setQuery(result.getQuery());
            hit.setSource(getName());
            hit.setQueryPacketData(queryPacketData);
            result.hits().add(hit);
        }

        if (resultPacket.getCoverageFeature()) {
            result.setCoverage(new Coverage(resultPacket.getCoverageDocs(), resultPacket.getActiveDocs(), resultPacket.getNodesReplied())
                    .setSoonActive(resultPacket.getSoonActiveDocs())
                    .setDegradedReason(resultPacket.getDegradedReason())
                    .setNodesTried(resultPacket.getNodesQueried()));
        }
    }

    static class FillHitResult {
        final boolean ok;
        final String error;
        FillHitResult(boolean ok) {
            this(ok, null);
        }
        FillHitResult(boolean ok, String error) {
            this.ok = ok;
            this.error = error;
        }
    }

    FillHitResult fillHit(FastHit hit, DocsumPacket packet, String summaryClass) {
        if (packet != null) {
            byte[] docsumdata = packet.getData();
            if (docsumdata.length > 0) {
                return new FillHitResult(true, decodeSummary(summaryClass, hit, docsumdata));
            }
        }
        return new FillHitResult(false);
    }

    static protected class FillHitsResult {
        public final int skippedHits; // Number of hits not producing a summary.
        public final String error; // Optional error message
        FillHitsResult(int skippedHits, String error) {
            this.skippedHits = skippedHits;
            this.error = error;
        }
    }
    /**
     * Fills the hits.
     *
     * @return the number of hits that we did not return data for, and an optional error message.
     *         when things are working normally we return 0.
     */
     public FillHitsResult fillHits(Result result, Packet[] packets, String summaryClass) throws IOException {
        int skippedHits = 0;
        String lastError = null;
        int packetIndex = 0;
        for (Iterator<Hit> i = hitIterator(result); i.hasNext();) {
            Hit hit = i.next();

            if (hit instanceof FastHit && ! hit.isFilled(summaryClass)) {
                FastHit fastHit = (FastHit) hit;

                packets[packetIndex].ensureInstanceOf(DocsumPacket.class, getName());
                DocsumPacket docsum = (DocsumPacket) packets[packetIndex];

                packetIndex++;
                FillHitResult fr = fillHit(fastHit, docsum, summaryClass);
                if ( ! fr.ok ) {
                    skippedHits++;
                }
                if (fr.error != null) {
                    result.hits().addError(ErrorMessage.createTimeout(fr.error));
                    skippedHits++;
                    lastError = fr.error;
                }
            }
        }
        result.hits().setSorted(false);
        return new FillHitsResult(skippedHits, lastError);
    }

    private boolean addCachedHits(Result result,
                                  PacketWrapper packetWrapper,
                                  String summaryClass,
                                  List<DocumentInfo> documents) {
        boolean filledAllOfEm = true;
        Query myQuery = result.getQuery();

        for (DocumentInfo document : documents) {
            FastHit hit = new FastHit();
            hit.setQuery(myQuery);

            hit.setFillable();
            hit.setCached(true);

            extractDocumentInfo(hit, document);

            DocsumPacket docsum = (DocsumPacket) packetWrapper.getPacket(document.getGlobalId(), document.getPartId(), summaryClass);

            if (docsum != null) {
                byte[] docsumdata = docsum.getData();

                if (docsumdata.length > 0) {
                    String error = decodeSummary(summaryClass, hit, docsumdata);
                    if (error != null) {
                        filledAllOfEm = false;
                    }
                } else {
                    filledAllOfEm = false;
                }
            } else {
                filledAllOfEm = false;
            }

            result.hits().add(hit);

        }

        return filledAllOfEm;
    }

    private void extractDocumentInfo(FastHit hit, DocumentInfo document) {
        hit.setSource(getName());

        Number rank = document.getMetric();

        hit.setRelevance(new Relevance(rank.doubleValue()));

        hit.setDistributionKey(document.getDistributionKey());
        hit.setGlobalId(document.getGlobalId());
        hit.setPartId(document.getPartId());
    }

    protected DocsumDefinitionSet getDocsumDefinitionSet(Query query) {
        DocumentDatabase db = getDocumentDatabase(query);
        return db.getDocsumDefinitionSet();
    }

    private String decodeSummary(String summaryClass, FastHit hit, byte[] docsumdata) {
        DocumentDatabase db = getDocumentDatabase(hit.getQuery());
        hit.setField(Hit.SDDOCNAME_FIELD, db.getName());
        return decodeSummary(summaryClass, hit, docsumdata, db.getDocsumDefinitionSet());
    }

    private String decodeSummary(String summaryClass, FastHit hit, byte[] docsumdata, DocsumDefinitionSet docsumSet) {
        String error = docsumSet.lazyDecode(summaryClass, docsumdata, hit);
        if (error == null) {
            hit.setFilled(summaryClass);
        }
        return error;
    }

    /**
     * Creates unfilled hits from a List of DocumentInfo instances. Do note
     * cacheKey should be available if a cache is active, even if the hit is not
     * created from a cache in the current call path.
     *
     * @param queryPacketData binary data from first phase of search, or null
     * @param cacheKey the key this hit should match in the packet cache, or null
     * @param channelDistributionKey distribution key of the node producing these hits.
     *                               Only set if produced directly by a search node, not dispatch
     *                               (in which case it is not set in the received packets.)
     */
    void addUnfilledHits(Result result,
                            List<DocumentInfo> documents,
                            boolean fromCache,
                            QueryPacketData queryPacketData,
                            CacheKey cacheKey,
                            Optional<Integer> channelDistributionKey) {
        Query myQuery = result.getQuery();

        for (DocumentInfo document : documents) {

            try {
                FastHit hit = new FastHit();
                hit.setQuery(myQuery);
                if (queryPacketData != null)
                    hit.setQueryPacketData(queryPacketData);
                hit.setCacheKey(cacheKey);

                hit.setFillable();
                hit.setCached(fromCache);

                extractDocumentInfo(hit, document);
                channelDistributionKey.ifPresent(hit::setDistributionKey);

                result.hits().add(hit);
            } catch (ConfigurationException e) {
                getLogger().log(LogLevel.WARNING, "Skipping hit", e);
            } catch (Exception e) {
                getLogger().log(LogLevel.ERROR, "Skipping malformed hit", e);
            }
        }
    }

    @SuppressWarnings("rawtypes")
    public static VespaBackEndSearcher getSearcher(String s) {
        try {
            Class c = Class.forName(s);
            if (VespaBackEndSearcher.class.isAssignableFrom(c)) {
                Constructor[] constructors = c.getConstructors();
                for (Constructor constructor : constructors) {
                    Class[] parameters = constructor.getParameterTypes();
                    if (parameters.length == 0) {
                        return (VespaBackEndSearcher) constructor.newInstance();
                    }
                }
                throw new RuntimeException("Failed initializing " + s);

            } else {
                 throw new RuntimeException(s + " is not com.yahoo.prelude.fastsearch.VespaBackEndSearcher");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failure loading class " + s + ", exception :" + e);
        }
    }

    protected boolean isLoggingFine() {
        return getLogger().isLoggable(Level.FINE);
    }

}
