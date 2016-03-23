#include "supersonic/base/infrastructure/block_util.h"

namespace supersonic {

const StorageType SharedStorageType::get() {
		return storage_type_;
}

void SharedStorageType::set(StorageType storage_type) {
	storage_type_ = storage_type;
}

void SharedStorageType::Reset() {
	storage_type_ = MEMORY;
}

StorageType SharedStorageType::storage_type_ = MEMORY;

}// namespace supersonic
