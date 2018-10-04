// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.vespa.hosted.controller.api.integration.dns;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A managed DNS service.
 *
 * @author mpolden
 */
public interface NameService {

    /**
     * Create a new CNAME record
     *
     * @param alias The alias to create
     * @param canonicalName The canonical name which the alias should point to. This must be a FQDN.
     */
    RecordId createCname(RecordName alias, RecordData canonicalName);

    /** Find record by type and name - will throw exception if more than one record matches */
    Optional<Record> findRecord(Record.Type type, RecordName name);

    /** Find record by type and name - may return multiple records */
    default List<Record> findRecords(Record.Type type, RecordName name) {
        List<Record> result = new ArrayList<>();
        findRecord(type, name).ifPresent(result::add);
        return result;
    }

    /** Find record by type and data */
    List<Record> findRecord(Record.Type type, RecordData data);

    /** Update existing record */
    void updateRecord(RecordId id, RecordData newData);

    /** Remove record by ID */
    void removeRecord(RecordId id);

}
