#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_BLOCK_UTIL_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_BLOCK_UTIL_H_

#include <iostream>
#include <sstream>
#include <string>
#include <memory>
using std::unique_ptr;

#include "supersonic/base/infrastructure/variant_pointer.h"
#include "supersonic/base/memory/arena.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/utils/file.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/scoped_ptr.h"

namespace supersonic {

// Number for the file name generation.
static int64 fileNameNum = 0;

// Type of the data storage.
enum StorageType {
	MEMORY, DISK, FLASH
};

class FileBuffer {
	// File buffer of for each column piece to buffer the data in disk.
public:
	const std::string file_name() const {
		return file_name_;
	}

	const size_t begin_in_file() const {
		return begin_in_file_;
	}

	const rowcount_t offset_buffered() const {
		return offset_buffered_;
	}
	const rowcount_t size_buffered() const {
		return size_buffered_;
	}

	const TypeInfo& type_info() const {
		return type_info_;
	}
	// Return the data pointer in the buffer.
	VariantConstPointer data() const {
		if(buffer_.get() == NULL) {
			return NULL;
		}else {
			return buffer_->data();
		}
	}

	// Reallocate requested memory for the buffer.
	bool reallocate(rowcount_t requested) {
		if(!allocator_->Reallocate(requested * type_info_.size(), buffer_.get())) {
			return false;
		}
		offset_buffered_ = 0;
		size_buffered_ = 0;
		return true;
	}

	// Prepare requested data
	// if the data has been buffered do nothing
	// else read data from the file to buffer.
	// offset_in_piece and requested are row_id in the column.
	rowcount_t PrepareData(rowcount_t offset_in_piece, rowcount_t requested) {
		rowcount_t end = requested + offset_in_piece;
		if(offset_in_piece >= offset_buffered_ && end < size_buffered_ + offset_buffered_) {
			return size_buffered_ - (offset_in_piece - offset_buffered_);
		}
		File* file = File::OpenOrDie(file_name_, "r");
		if(!type_info_.is_variable_length()) {
			size_t offset_in_file = begin_in_file_ + offset_in_piece * type_info_.size();
			file->Seek(offset_in_file);
			if(reallocate(requested)){
				size_buffered_ = file->Read(buffer_->data(), requested * type_info_.size()) / type_info_.size();
				offset_buffered_ = offset_in_piece;
				file->Close();
				return size_buffered_;
			}else {
				std::cerr<<"reallocate fail in PrepareData"<<std::endl;
				offset_buffered_ = 0;
				size_buffered_ = 0;
				file->Close();
				return 0;
			}
		}else {
			size_t first_offset = 0;
			size_t data_length = 0;
			size_t* data_index = new size_t[2 * requested];
			file->Seek(begin_in_file_ + offset_in_piece * 2 * sizeof(size_t));
			file->Read(data_index, 2 * requested * sizeof(size_t));
			first_offset = begin_in_file_ + data_index[0];
			for(rowcount_t i = 0; i < requested; i++) {
				data_length += data_index[2 * i + 1];
			}
			char* strings_data = NULL;
			strings_data = static_cast<char*>(arena_->AllocateBytes(data_length));
			if(!strings_data) {
				std::cerr << "Arena allocation for filebuffer failed."<< std::endl;
			}
			file->Seek(first_offset);
			size_t read_length = file->Read(strings_data, data_length);
			CHECK_EQ(read_length, data_length);
			if(reallocate(requested)){
				StringPiece* string_pieces = static_cast<StringPiece*>(buffer_->data());
				size_t offset = 0;
				for(rowcount_t row = 0; row < requested; row++) {
					string_pieces[row] = StringPiece(strings_data + offset, data_index[2 * row + 1]);
					offset += data_index[2 * row + 1];
				}
				size_buffered_ = requested;
				offset_buffered_ = offset_in_piece;
				file->Close();
				return size_buffered_;
			}else {
				std::cerr<<"reallocate fail in PrepareData (variable length)"<<std::endl;
				offset_buffered_ = 0;
				size_buffered_ = 0;
				file->Close();
				return 0;
			}
		}
	}

private:
	// Only the ColumnPiece to create FileBuffer.
	friend class ColumnPiece;

	FileBuffer(VariantConstPointer data, rowcount_t length, const TypeInfo& type_info)
	: file_name_(AllocateFileName()),
	  allocator_(HeapBufferAllocator::Get()),
	  type_info_(type_info),
	  arena_(nullptr) {
		File* file = File::OpenOrDie(file_name_, "w+");
		begin_in_file_  = file->FileSize();
		file->Seek(begin_in_file_);
		if(!type_info.is_variable_length()) {
			size_t write_size = file->Write(data.raw(), length * type_info.size());
			CHECK_EQ(write_size, length * type_info.size()) << "in File Buffer write_size != length";
		} else {
			const StringPiece* string_piece_ptr = data.as_variable_length();
			size_t* data_index = new size_t[2 * length];
			size_t total_length = 0;
			for(rowcount_t i = 0; i < length; i++) {
				data_index[2 * i ] = total_length + 2 * length * sizeof(size_t);
				data_index[2 * i + 1] = string_piece_ptr[i].length();
				total_length += data_index[2 * i + 1];
			}
			size_t write_size = file->Write(data_index, 2 * length * sizeof(size_t));
			CHECK_EQ(write_size, 2 * length * sizeof(size_t)) << "in File Buffer write_size != 2 * length * sizeof(size_t)";
			for(rowcount_t i = 0; i < length; i++) {
				write_size = file->Write(string_piece_ptr[i].data(), string_piece_ptr[i].length());
				CHECK_EQ(write_size, string_piece_ptr[i].length()) << "in File Buffer write_size != string_piece_ptr[i].length()";
			}
			delete data_index;
		}
		file->Close();
		Init();
	}

	FileBuffer(const FileBuffer& file_buffer)
	:	file_name_(file_buffer.file_name()),
		begin_in_file_(file_buffer.begin_in_file()),
		allocator_(HeapBufferAllocator::Get()),
		type_info_(file_buffer.type_info()),
		arena_(nullptr) {
		Init();
	}
	// Generate the name for data file in disk.
	// TODO(axyu) rewrite this function to avoid generating large number of small files.
	std::string AllocateFileName(){
		fileNameNum++;
		std::string directory = "../storage/";
		std::string prefix = "column_piece_";
		std::stringstream ss;
		std::string suffix =  ".temp";
		ss << directory << prefix << fileNameNum << suffix;
		return ss.str();
	}
	// Init the buffer_.
	void Init() {
		buffer_.reset(allocator_->Allocate(0));
		if(type_info_.is_variable_length())
			arena_.reset(new Arena(allocator_, 0, kMaxArenaBufferSize));
		offset_buffered_ = 0;
		size_buffered_ = 0;
	}

	// Name of the data file in disk.
	const std::string file_name_;
	// Begin offset of the data in the file.
	size_t begin_in_file_;
	// Offset of the data which has been buffered into memory(row_id).
	rowcount_t offset_buffered_;
	// Size of the data which has been buffered into memory(row_id).
	rowcount_t size_buffered_;
	// Allocator for the buffer.
	BufferAllocator* const allocator_;
	// Buffer for the data in disk.
	scoped_ptr<Buffer> buffer_;
	// Type info of the data.
	const TypeInfo& type_info_;
	// Holds variable length data
	scoped_ptr<Arena> arena_;
};

class ColumnPiece {
public:
	ColumnPiece(VariantConstPointer data_in_memory,
			const rowcount_t offset,
			const rowcount_t size,
			const TypeInfo& type_info)
: data_in_memory_(data_in_memory),
  in_memory_offset_(offset),
  offset_(offset),
  size_(size),
  type_info_(&type_info),
  storage_type_(MEMORY),
  file_buffer_(nullptr) {}

	// Construct ColumnPiece from another ColumnPiece.
	ColumnPiece(const ColumnPiece& source_column_piece,
			VariantConstPointer data_in_memory,
			const rowcount_t in_memory_offset,
			const rowcount_t offset,
			const StorageType storage_type,
			const TypeInfo& type_info)
	: data_in_memory_(data_in_memory),
	  in_memory_offset_(in_memory_offset),
	  offset_(offset),
	  type_info_(&type_info),
	  size_(source_column_piece.size()),
	  storage_type_(storage_type),
	  file_buffer_(nullptr) {
		if(source_column_piece.storage_type() == MEMORY) {
			if(storage_type == MEMORY) {
				// MEMORY to MEMORY: Do nothing.
			}
			else {
				// MEMORY to DISK: Create the file buffer.
				file_buffer_.reset(new FileBuffer(source_column_piece.data(), source_column_piece.size(), type_info));
			}
		}else {
			if(storage_type == MEMORY) {
				// DISK to MEMORY: This situation should not appear.
				std::cerr<<"in ColumnPiece disk -> memory "<<std::endl;
				exit(-1);
			}else {
				// DISK to DISK: Share the same data file in disk.
				file_buffer_.reset(new FileBuffer(source_column_piece.file_buffer()));
			}
		}
	}

	// If the data is storaged in MEMORY, return the data
	// else return the pointer which points to the buffer.
	VariantConstPointer data() const {
		if(storage_type_ == MEMORY){
			return data_in_memory_;
		}else {
			return file_buffer_->data();
		}
	}

	// Return the pointer of the data plus an offset.
	VariantConstPointer data_plus_offset(const rowcount_t offset_in_piece) const {
		if(storage_type_ == MEMORY) {
			return data_in_memory_.offset(offset_in_piece, *type_info_);
		}else {
			if(file_buffer_->PrepareData(offset_in_piece, 1) == 1) {
				//return file_buffer_->data().offset(offset_in_piece, *type_info_);
				return file_buffer_->data();
			}else {
				return NULL;
			}
		}
	}

	rowcount_t in_memory_offset() const {
		return in_memory_offset_;
	}

	rowcount_t offset() const {
		return offset_;
	}

	rowcount_t size() const {
		return size_;
	}

	StorageType storage_type() const {
		return storage_type_;
	}

	const TypeInfo& type_info() const {
		return *type_info_;
	}

	void Reset(VariantConstPointer data) {
		if(storage_type_ == MEMORY) {
			data_in_memory_ = data;
		}
	}

	// Buffer all data in disk into the buffer.
	rowcount_t PrepareData() const {
		if(storage_type_ == MEMORY) {
			return size_;
		}else {
			return file_buffer_->PrepareData(0, size_);
		}
	}

private:
	const FileBuffer& file_buffer() const {
		return *file_buffer_;
	}
	// Pointer to the data stored in memory.
	VariantConstPointer data_in_memory_;
	// Offset in memory of the piece.
	rowcount_t in_memory_offset_;
	// Offset of the column.
	rowcount_t offset_;
	// type_info of the column.
	const TypeInfo* type_info_;
	// Size of the piece.
	rowcount_t size_;
	// Storage type of the piece.
	StorageType storage_type_;
	// Pointer to the file buffer, owned and only owned by this column piece.
	unique_ptr<FileBuffer> file_buffer_;

	DISALLOW_COPY_AND_ASSIGN(ColumnPiece);
};

// A auxiliary class for column copier.
class SharedStorageType {
public:
	static const StorageType get();

private:
	friend class BaseViewCopier;

	static void set(StorageType storage_type);
	static void Reset() ;
	static StorageType storage_type_;
};

// Print the infomation of the column piece to do test.
template<DataType type>
void PrintColumnPiece(const ColumnPiece& column_piece) {
	vector<string> storage_type({"MEMORY", "DISK", "FLASH"});
	std::cout << "******Info of This Column Piece Begin******" << std::endl;
	std::cout << "This column piece is stored in: " << storage_type[column_piece.storage_type()] << std::endl;
	if(column_piece.storage_type() == MEMORY) {
		std::cout << "This column piece's offset in memory is: " << column_piece.in_memory_offset() << std::endl;
	}
	std::cout << "This column piece's size is: " << column_piece.size() << std::endl;
	std::cout << ">>>>>>Data in This Column Piece Begin<<<<<<" << std::endl;
	for(size_t i = 0; i < column_piece.size(); i ++) {
		const typename TypeTraits<type>::cpp_type* data = column_piece.data_plus_offset(i).as<type>();
		std::cout << "Row offset: " << column_piece.offset() + i << " Data: " << *data << std::endl;
	}
	std::cout << ">>>>>>Data in This Column Piece End<<<<<<" << std::endl;
	std::cout << "******Info of This Column Piece End******" << std::endl;
}

}// namespace supersonic

#endif
