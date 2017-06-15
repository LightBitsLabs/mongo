/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/storage/devnull/devnull_kv_engine.h"

#include "mongo/base/disallow_copying.h"
#include "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_record_store.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/stdx/memory.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/index/index_descriptor.h"


mongo::BSONObj stripFieldNames( const mongo::BSONObj& obj ) {
	mongo::BSONObjBuilder b;
	mongo::BSONObjIterator i( obj );
   while ( i.more() ) {
	   mongo::BSONElement e = i.next();
	   b.appendAs( e, "" );
   }
   return b.obj();
}

namespace mongo {

class EmptyRecordCursor final : public SeekableRecordCursor {
public:
    EmptyRecordCursor(memcached_pool_st *pool):_memcached_pool(pool){

    }
    boost::optional<Record> next() final {
        return {};
    }
    boost::optional<Record> seekExact(const RecordId& id) final {
	   memcached_return_t rc;
	   auto memc = memcached_pool_pop(_memcached_pool, true, &rc);
	   assert(rc == 0);

	   std::string key = std::to_string(id.repr());
	   size_t val_len;
	   char *value =  memcached_get(memc, key.c_str(), key.length(), &val_len, nullptr, &rc);

	   if (rc == MEMCACHED_NOTFOUND) {
		memcached_pool_push(_memcached_pool, memc);
	   	return {};
	   }

	   memcached_pool_push(_memcached_pool, memc);
	   Record result = {id, {value, static_cast<int>(val_len)}};

	   // copy the result
	   result.data.makeOwned();
	   free(value);
	   return result;
    }

    void save() final {}
    bool restore() final {
        return true;
    }
    void detachFromOperationContext() final {}
    void reattachToOperationContext(OperationContext* txn) final {}

private :
    memcached_pool_st *_memcached_pool;
};


class MemcachedCursor final : public SortedDataInterface::Cursor {
public:
	MemcachedCursor(memcached_pool_st* memcached_pool, Ordering order):_memcached_pool(memcached_pool), _order(order), _query(KeyString::Version::V1){}

	virtual boost::optional<IndexKeyEntry> seekExact(const BSONObj& key,
	                                                 RequestedInfo parts = kKeyAndLoc) {
		_query.resetToKey(stripFieldNames(key), _order);
		std::string searchKey(_query.getBuffer(), _query.getSize());
		 memcached_return_t rc;
		auto memc = memcached_pool_pop(_memcached_pool, true, &rc);
		assert(rc == 0);


		size_t val_len;
		char *value =  memcached_get(memc, searchKey.c_str(), searchKey.length(), &val_len, nullptr, &rc);

		if (rc == MEMCACHED_NOTFOUND) {
			memcached_pool_push(_memcached_pool, memc);
			return {};
		}

		BufReader br(value, val_len);
		auto rec_id = KeyString::decodeRecordId(&br);

		_value = value;
		free(value);
		memcached_pool_push(_memcached_pool, memc);

		return IndexKeyEntry(key, rec_id);

	}

	 virtual void setEndPosition(const BSONObj& key, bool inclusive)
	 {
		 assert(0);
	 }


	 virtual boost::optional<IndexKeyEntry> next(RequestedInfo parts = kKeyAndLoc)
	 {
		 assert(0);
		 return {};
	 }

	 virtual boost::optional<IndexKeyEntry> seek(const BSONObj& key,
						    bool inclusive,
						    RequestedInfo parts = kKeyAndLoc)
	 {
		 assert(0);
		 return {};
	 }


	virtual boost::optional<IndexKeyEntry> seek(const IndexSeekPoint& seekPoint,
						    RequestedInfo parts = kKeyAndLoc)
	{
		assert(0);
		return {};
	}


	 virtual void save()
	 {
		 assert(0);
	 }


	 virtual void restore()
	 {
		 assert(0);
	 }


	 virtual void detachFromOperationContext()
	 {
		 assert(0);
	 }

	 virtual void reattachToOperationContext(OperationContext* opCtx)
	 {
		 assert(0);
	 }

private:
	memcached_pool_st* _memcached_pool;
	Ordering _order;
	KeyString _query;
	std::string _value;

};

class DevNullRecordStore : public RecordStore {
public:
    DevNullRecordStore(memcached_pool_st* memcached_pool, StringData ns, const CollectionOptions& options)
        : RecordStore(ns), _options(options) {
        _numInserts = 0;
        _dummy = BSON("_id" << 1);
        _nextIdNum.store(1);
        _memcached_pool = memcached_pool;

    }

    virtual const char* name() const {
        return "devnull";
    }

    virtual void setCappedCallback(CappedCallback*) {}

    virtual long long dataSize(OperationContext* txn) const {
        return 0;
    }

    virtual long long numRecords(OperationContext* txn) const {
        return 0;
    }

    virtual bool isCapped() const {
        return _options.capped;
    }

    virtual int64_t storageSize(OperationContext* txn,
                                BSONObjBuilder* extraInfo = NULL,
                                int infoLevel = 0) const {
        return 0;
    }

    virtual RecordData dataFor(OperationContext* txn, const RecordId& loc) const {
        return RecordData(_dummy.objdata(), _dummy.objsize());
    }

    virtual bool findRecord(OperationContext* txn, const RecordId& loc, RecordData* rd) const {
        return false;
    }

    virtual void deleteRecord(OperationContext* txn, const RecordId& dl) {}


    virtual StatusWith<RecordId> insertRecord(OperationContext* txn,
                                              const char* data,
                                              int len,
                                              bool enforceQuota) {
        _numInserts++;
        RecordId rec_info(_nextIdNum.fetchAndAdd(1));
        memcached_return_t rc;

        auto memc = memcached_pool_pop(_memcached_pool, true, &rc);
        assert(rc == 0);

        std::string key = std::to_string(rec_info.repr());

        rc = memcached_set(memc, key.c_str(), key.length(), data, len, 0 , 0);
        assert(rc == 0 || rc == MEMCACHED_BUFFERED);
        memcached_pool_push(_memcached_pool, memc);
        return StatusWith<RecordId>(rec_info);
    }

    virtual Status insertRecordsWithDocWriter(OperationContext* txn,
                                              const DocWriter* const* docs,
                                              size_t nDocs,
                                              RecordId* idsOut) {
        _numInserts += nDocs;
        if (idsOut) {
            for (size_t i = 0; i < nDocs; i++) {
                idsOut[i] = RecordId(6, 4);
            }
        }
        return Status::OK();
    }

    virtual Status updateRecord(OperationContext* txn,
                                const RecordId& oldLocation,
                                const char* data,
                                int len,
                                bool enforceQuota,
                                UpdateNotifier* notifier) {

	    memcached_return_t rc;

	    auto memc = memcached_pool_pop(_memcached_pool, true, &rc);
	    assert(rc == 0);

	    std::string key = std::to_string(oldLocation.repr());

	    rc = memcached_set(memc, key.c_str(), key.length(), data, len, 0 , 0);
	    assert(rc == 0 || rc == MEMCACHED_BUFFERED);
	    memcached_pool_push(_memcached_pool, memc);
	    return Status::OK();
    }

    virtual bool updateWithDamagesSupported() const {
        return false;
    }

    virtual StatusWith<RecordData> updateWithDamages(OperationContext* txn,
                                                     const RecordId& loc,
                                                     const RecordData& oldRec,
                                                     const char* damageSource,
                                                     const mutablebson::DamageVector& damages) {
        invariant(false);
    }


    std::unique_ptr<SeekableRecordCursor> getCursor(OperationContext* txn,
                                                    bool forward) const final {
        return stdx::make_unique<EmptyRecordCursor>(_memcached_pool);
    }

    virtual Status truncate(OperationContext* txn) {
        return Status::OK();
    }

    virtual void temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive) {}

    virtual Status validate(OperationContext* txn,
                            ValidateCmdLevel level,
                            ValidateAdaptor* adaptor,
                            ValidateResults* results,
                            BSONObjBuilder* output) {
        return Status::OK();
    }

    virtual void appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* result,
                                   double scale) const {
        result->appendNumber("numInserts", _numInserts);
    }

    virtual Status touch(OperationContext* txn, BSONObjBuilder* output) const {
        return Status::OK();
    }

    void waitForAllEarlierOplogWritesToBeVisible(OperationContext* txn) const override {}

    virtual void updateStatsAfterRepair(OperationContext* txn,
                                        long long numRecords,
                                        long long dataSize) {}

private:
    CollectionOptions _options;
    long long _numInserts;
    AtomicUInt64 _nextIdNum;
    BSONObj _dummy;
    memcached_pool_st* _memcached_pool;
};

class DevNullSortedDataBuilderInterface : public SortedDataBuilderInterface {
    MONGO_DISALLOW_COPYING(DevNullSortedDataBuilderInterface);

public:
    DevNullSortedDataBuilderInterface() {}

    virtual Status addKey(const BSONObj& key, const RecordId& loc) {
        return Status::OK();
    }
};

class DevNullSortedDataInterface : public SortedDataInterface {
public:
    DevNullSortedDataInterface( memcached_pool_st* memcached_pool, Ordering order) : _memcached_pool(memcached_pool), _order(order){}
    virtual ~DevNullSortedDataInterface() {}

    virtual SortedDataBuilderInterface* getBulkBuilder(OperationContext* txn, bool dupsAllowed) {
        return new DevNullSortedDataBuilderInterface();
    }

    virtual Status insert(OperationContext* txn,
                          const BSONObj& key,
                          const RecordId& loc,
                          bool dupsAllowed) {

	    memcached_return_t rc;
	    KeyString encodedKey(KeyString::Version::V1, key, _order);

	    auto memc = memcached_pool_pop(_memcached_pool, true, &rc);
	    assert(rc == 0);


	    KeyString value(KeyString::Version::V1, loc);
	   // if (!encodedKey.getTypeBits().isAllZeros()) {
	    	    value.appendTypeBits(encodedKey.getTypeBits());
	    //}
 	    rc = memcached_set(memc, encodedKey.getBuffer(), encodedKey.getSize(), value.getBuffer(), value.getSize(), 0 , 0);
            assert(rc == 0 || rc == MEMCACHED_BUFFERED);
            memcached_pool_push(_memcached_pool, memc);
            return Status::OK();
    }

    virtual void unindex(OperationContext* txn,
                         const BSONObj& key,
                         const RecordId& loc,
                         bool dupsAllowed) {}

    virtual Status dupKeyCheck(OperationContext* txn, const BSONObj& key, const RecordId& loc) {
        return Status::OK();
    }

    virtual void fullValidate(OperationContext* txn,
                              long long* numKeysOut,
                              ValidateResults* fullResults) const {}

    virtual bool appendCustomStats(OperationContext* txn,
                                   BSONObjBuilder* output,
                                   double scale) const {
        return false;
    }

    virtual long long getSpaceUsedBytes(OperationContext* txn) const {
        return 0;
    }

    virtual bool isEmpty(OperationContext* txn) {
        return true;
    }

    virtual std::unique_ptr<SortedDataInterface::Cursor> newCursor(OperationContext* txn,
                                                                   bool isForward) const {
	    return stdx::make_unique<MemcachedCursor>(_memcached_pool, _order);
    }

    virtual Status initAsEmpty(OperationContext* txn) {
        return Status::OK();
    }
    memcached_pool_st* _memcached_pool;
    Ordering _order;
};

DevNullKVEngine::DevNullKVEngine(){
	std::string lightbox_config = std::string("--SERVER=") + std::string(getenv( "KV_IPADDR" ) ?: "");
	memc = memcached(lightbox_config.c_str(), lightbox_config.length());
	memcached_return_t rc;

	//rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_TCP_NODELAY, 1);
	//assert(rc == 0);
	rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_BINARY_PROTOCOL, 1);
	assert(rc == 0);
	rc = memcached_behavior_set(memc, MEMCACHED_BEHAVIOR_NOREPLY, 1);
	assert(rc == 0);

	assert(memc);
	memcached_pool = memcached_pool_create(memc, 10, 1000);
	assert(memcached_pool);
}

std::unique_ptr<RecordStore> DevNullKVEngine::getRecordStore(OperationContext* opCtx,
                                                             StringData ns,
                                                             StringData ident,
                                                             const CollectionOptions& options) {
    if (ident == "_mdb_catalog") {
        return stdx::make_unique<EphemeralForTestRecordStore>(ns, &_catalogInfo);
    }
    return stdx::make_unique<DevNullRecordStore>(memcached_pool, ns, options);
}

SortedDataInterface* DevNullKVEngine::getSortedDataInterface(OperationContext* opCtx,
                                                             StringData ident,
                                                             const IndexDescriptor* desc) {
    return new DevNullSortedDataInterface(memcached_pool, Ordering::make(desc->keyPattern()));
}
}
