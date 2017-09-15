// Copyright 2017 Yahoo Holdings. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.

#include "gid_to_lid_change_handler.h"
#include "i_gid_to_lid_change_listener.h"
#include <vespa/searchlib/common/lambdatask.h>
#include <vespa/searchcorespi/index/i_thread_service.h>
#include <vespa/document/base/globalid.h>
#include <cassert>
#include <vespa/vespalib/stllike/hash_map.hpp>

using search::makeLambdaTask;


namespace proton {

GidToLidChangeHandler::GidToLidChangeHandler()
    : _lock(),
      _listeners(),
      _closed(false),
      _pendingRemove()

{
}


GidToLidChangeHandler::~GidToLidChangeHandler()
{
    assert(_closed);
    assert(_listeners.empty());
    assert(_pendingRemove.empty());
}

void
GidToLidChangeHandler::notifyPutDone(GlobalId gid, uint32_t lid)
{
    for (const auto &listener : _listeners) {
        listener->notifyPutDone(gid, lid);
    }
}

void
GidToLidChangeHandler::notifyRemove(GlobalId gid)
{
    for (const auto &listener : _listeners) {
        listener->notifyRemove(gid);
    }
}

void
GidToLidChangeHandler::notifyPutDone(GlobalId gid, uint32_t lid, SerialNum serialNum)
{
    lock_guard guard(_lock);
    auto itr = _pendingRemove.find(gid);
    if (itr != _pendingRemove.end()) {
        auto &entry = itr->second;
        assert(entry.removeSerialNum != serialNum);
        if (entry.removeSerialNum > serialNum) {
            return; // Document has already been removed later on
        }
        assert(entry.putSerialNum < serialNum);
        entry.putSerialNum = serialNum;
    }
    notifyPutDone(gid, lid);
}

void
GidToLidChangeHandler::notifyRemove(GlobalId gid, SerialNum serialNum)
{
    lock_guard guard(_lock);
    auto insRes = _pendingRemove.insert(std::make_pair(gid, PendingRemoveEntry(serialNum)));
    if (!insRes.second) {
        auto &entry = insRes.first->second;
        assert(entry.removeSerialNum < serialNum);
        assert(entry.putSerialNum < serialNum);
        if (entry.removeSerialNum < entry.putSerialNum) {
            notifyRemove(gid);
        }
        entry.removeSerialNum = serialNum;
        ++entry.refCount;
    } else {
        notifyRemove(gid);
    }
}

void
GidToLidChangeHandler::notifyRemoveDone(GlobalId gid, SerialNum serialNum)
{
    lock_guard guard(_lock);
    auto itr = _pendingRemove.find(gid);
    assert(itr != _pendingRemove.end());
    auto &entry = itr->second;
    assert(entry.removeSerialNum >= serialNum);
    if (entry.refCount == 1) {
        _pendingRemove.erase(itr);
    } else {
        --entry.refCount;
    }
}

void
GidToLidChangeHandler::close()
{
    Listeners deferredDelete;
    {
        lock_guard guard(_lock);
        _closed = true;
        _listeners.swap(deferredDelete);
    }
}

void
GidToLidChangeHandler::addListener(std::unique_ptr<IGidToLidChangeListener> listener)
{
    lock_guard guard(_lock);
    if (!_closed) {
        const vespalib::string &docTypeName = listener->getDocTypeName();
        const vespalib::string &name = listener->getName();
        for (const auto &oldlistener : _listeners) {
            if (oldlistener->getDocTypeName() == docTypeName && oldlistener->getName() == name) {
                return;
            }
        }
        _listeners.emplace_back(std::move(listener));
        _listeners.back()->notifyRegistered();
    } else {
        assert(_listeners.empty());
    }
}

namespace {

bool shouldRemoveListener(const IGidToLidChangeListener &listener,
                          const vespalib::string &docTypeName,
                          const std::set<vespalib::string> &keepNames)
{
    return ((listener.getDocTypeName() == docTypeName) &&
            (keepNames.find(listener.getName()) == keepNames.end()));
}

}

void
GidToLidChangeHandler::removeListeners(const vespalib::string &docTypeName,
                                       const std::set<vespalib::string> &keepNames)
{
    Listeners deferredDelete;
    {
        lock_guard guard(_lock);
        if (!_closed) {
            auto itr = _listeners.begin();
            while (itr != _listeners.end()) {
                if (shouldRemoveListener(**itr, docTypeName, keepNames)) {
                    deferredDelete.emplace_back(std::move(*itr));
                    itr = _listeners.erase(itr);
                } else {
                    ++itr;
                }
            }
        } else {
            assert(_listeners.empty());
        }
    }
}

} // namespace proton
