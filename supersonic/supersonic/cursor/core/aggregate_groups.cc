// Copyright 2010 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include <stddef.h>
#include <algorithm>
#include "supersonic/utils/std_namespace.h"
#include <list>
#include "supersonic/utils/std_namespace.h"
#include <memory>
#include <set>
#include "supersonic/utils/std_namespace.h"
#include <sstream>
using std::stringstream;
#include <vector>
using std::vector;
#include <pthread.h>

#include <glog/logging.h>
#include "supersonic/utils/logging-inl.h"
#include "supersonic/utils/macros.h"
#include "supersonic/utils/port.h"
#include "supersonic/utils/scoped_ptr.h"
#include "supersonic/utils/stringprintf.h"
#include "supersonic/utils/exception/failureor.h"
#include "supersonic/base/exception/exception.h"
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/bit_pointers.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/base/infrastructure/types.h"
#include "supersonic/base/memory/memory.h"
#include "supersonic/cursor/base/cursor.h"
#include "supersonic/cursor/base/operation.h"
#include "supersonic/cursor/proto/cursors.pb.h"
#include "supersonic/cursor/core/aggregate.h"
#include "supersonic/cursor/core/aggregator.h"
#include "supersonic/cursor/core/hybrid_group_utils.h"
#include "supersonic/cursor/core/sort.h"
#include "supersonic/cursor/infrastructure/basic_cursor.h"
#include "supersonic/cursor/infrastructure/basic_operation.h"
#include "supersonic/cursor/infrastructure/iterators.h"
#include "supersonic/cursor/infrastructure/ordering.h"
#include "supersonic/cursor/infrastructure/row_hash_set.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/cursor/infrastructure/view_printer.h"
#include "supersonic/proto/supersonic.pb.h"
#include "supersonic/utils/strings/join.h"
#include "supersonic/utils/container_literal.h"
#include "supersonic/utils/map_util.h"
#include "supersonic/utils/pointer_vector.h"
#include "supersonic/utils/stl_util.h"

namespace supersonic {

	class TupleSchema;

	namespace {

		void* thread(void* arg);

		// Creates and updates a block of unique keys that are the result of grouping.
		class GroupKeySet {
			public:
				typedef row_hash_set::FindResult FindResult;  // Re-exporting.

				// Creates a GroupKeySet. group_by_columns describes which columns constitute
				// a key and should be grouped together, it can be empty, in which case all
				// rows are considered equal and are grouped together. Set is pre-allocated
				// to store initial_row_capacity unique keys, and it can grow as needed.
				static FailureOrOwned<GroupKeySet> Create(
						const BoundSingleSourceProjector* group_by,
						BufferAllocator* allocator,
						rowcount_t initial_row_capacity,
						const int64 max_unique_keys_in_result) {
					std::unique_ptr<GroupKeySet> group_key_set(
							new GroupKeySet(group_by, allocator, max_unique_keys_in_result));
					PROPAGATE_ON_FAILURE(group_key_set->Init(initial_row_capacity));
					return Success(group_key_set.release());
				}

				const TupleSchema& key_schema() const {
					return key_projector_->result_schema();
				}

				// View on a block that keeps unique keys. Can be called only when key is not
				// empty.
				const View& key_view() const {
					return key_row_set_.indexed_view();
				}

				// How many rows can a view passed in the next call to Insert() have.
				// TODO(user): Remove this limitation (the row hash set could use a loop
				// to insert more items that it can process in a single shot).
				rowcount_t max_view_row_count_to_insert() const {
					// Does not accept views larger then cursor::kDefaultRowCount because this
					// is the size of preallocated table that holds result of Insert().
					//return Cursor::kDefaultRowCount;
					return Optimizer::kRowGroupSize;
				}

				// Count of unique keys in the set.
				rowcount_t size() const { return key_row_set_.size(); }

				// Inserts all unique keys from the view to the key block. For each row
				// from input view finds an index of its key in the key block and puts
				// these indexes in the result table.
				// Input view can not have more rows then max_view_row_count_to_insert().
				const rowid_t Insert(const View& view, FindResult* result) {
					CHECK_LE(view.row_count(), max_view_row_count_to_insert());
					key_projector_->Project(view, &child_key_view_);
					child_key_view_.set_row_count(view.row_count());
					return key_row_set_.Insert(child_key_view_, result);
				}

				// Insert selective unique keys from the view to the key block.
				const rowid_t Insert(const View& view, const bool_const_ptr selection_vector, FindResult* result) {
					CHECK_LE(view.row_count(), max_view_row_count_to_insert());
					key_projector_->Project(view, &child_key_view_);
					child_key_view_.set_row_count(view.row_count());
					return key_row_set_.Insert(child_key_view_, selection_vector, result);
				}

				void Reset() { key_row_set_.Clear(); }

				void Compact() { key_row_set_.Compact(); }

				const BoundSingleSourceProjector* key_projector() {
					return key_projector_.get();
				}

			private:
				GroupKeySet(const BoundSingleSourceProjector* group_by,
						BufferAllocator* allocator,
						const int64 max_unique_keys_in_result)
					: key_projector_(group_by),
					child_key_view_(key_projector_->result_schema()),
					key_row_set_(key_projector_->result_schema(), allocator,
							max_unique_keys_in_result) {}

				FailureOrVoid Init(rowcount_t reserved_capacity) {
					if (!key_row_set_.ReserveRowCapacity(reserved_capacity)) {
						THROW(new Exception(
									ERROR_MEMORY_EXCEEDED,
									StrCat("Block allocation failed. Key block with capacity ",
										reserved_capacity, " not allocated.")));
					}
					return Success();
				}

				std::unique_ptr<const BoundSingleSourceProjector> key_projector_;

				// View over an input view from child but with only key columns.
				View child_key_view_;

				row_hash_set::RowHashSet key_row_set_;

				DISALLOW_COPY_AND_ASSIGN(GroupKeySet);
		};

		// Cursor that is used for handling the standard GroupAggregate mode and
		// BestEffortGroupAggregate mode. The difference between these two modes is that
		// GroupAggregate needs to process the whole input during creation (returns out
		// of memory error if aggregation result is too large) and
		// BestEffortGroupAggregate does not process anything during creation and
		// processes as large chunks as possible during iteration phase, but does not
		// guarantee that the final result will be fully aggregated (i.e. there can be
		// more than one output for a given key). To make BestEffortGroupAggregate
		// deterministic (always producing the same output), pass GuaranteeMemory as its
		// allocator.
		class MeasureAggregateCursor : public BasicCursor {
			public:
				// Creates the cursor. Returns immediately (w/o processing any input).
				static FailureOrOwned<MeasureAggregateCursor> Create(
						const BoundSingleSourceProjector* group_by,
						vector<int> group_id,
						Aggregator* aggregator,
						BufferAllocator* allocator,           // Takes ownership
						BufferAllocator* original_allocator,  // Doesn't take ownership.
						bool best_effort,
						const int64 max_unique_keys_in_result,
						const vector<vector<int>>* filter_order,
						vector<const View*> group_vectors,
						Cursor* child) {
					std::unique_ptr<BufferAllocator> allocator_owner(CHECK_NOTNULL(allocator));
					vector<const View*> group_vectors_owner(group_vectors);

					std::unique_ptr<TupleSchema> group_schema(new TupleSchema());
					for(int i = 0; i < group_id.size(); i++) {
						group_schema->add_attribute(group_vectors[group_id[i]]->column(0).attribute());
					}

					std::unique_ptr<Cursor> child_owner(child);
					std::unique_ptr<Aggregator> aggregator_owner(aggregator);

					std::unique_ptr<const SingleSourceProjector> key_projector(ProjectAllAttributes());
					FailureOrOwned<const BoundSingleSourceProjector> bound_key_projector(
							key_projector->Bind(*group_schema));
					PROPAGATE_ON_FAILURE(bound_key_projector);

					FailureOrOwned<GroupKeySet> key = GroupKeySet::Create(
							bound_key_projector.release(), allocator_owner.get(), aggregator_owner->capacity(),
							max_unique_keys_in_result);
					PROPAGATE_ON_FAILURE(key);

					vector<const TupleSchema*> input_schemas(2);
					input_schemas[0] = group_schema.get();
					input_schemas[1] = &aggregator->schema();
					std::unique_ptr<MultiSourceProjector> result_projector(
							(new CompoundMultiSourceProjector())
							->add(0, ProjectAllAttributes())
							->add(1, ProjectAllAttributes()));
					FailureOrOwned<const BoundMultiSourceProjector> bound_result_projector(
							result_projector->Bind(input_schemas));
					PROPAGATE_ON_FAILURE(bound_result_projector);
					TupleSchema result_schema = bound_result_projector->result_schema();
					return Success(
							new MeasureAggregateCursor(result_schema,
								allocator_owner.release(),  // Takes ownership.
								original_allocator,
								filter_order,
								group_vectors_owner,
								group_by,
								group_id,
								key.release(),
								*group_schema,
								aggregator_owner.release(),
								bound_result_projector.release(),
								best_effort,
								max_unique_keys_in_result,
								child_owner.release()));
				}

				virtual ResultView Next(rowcount_t max_row_count) {
					while (true) {
						if (result_.next(max_row_count)) {
							return ResultView::Success(&result_.view());
						}
						if (input_exhausted_) return ResultView::EOS();
						// No rows from this call, yet input not exhausted. Retry.
						PROPAGATE_ON_FAILURE(ProcessInput());
						if (child_.is_waiting_on_barrier()) return ResultView::WaitingOnBarrier();
					}
				}

				// If false, the Cursor will not return more data above what was already
				// returned from Next() calls (unless TruncateResultView is called). This
				// method can be used to determine if best-effort group managed to do full
				// grouping:
				// - Call .Next(numeric_limits<rowcount_t>::max())
				// - Now if CanReturnMoreData() == false, we know that all the results of
				// best-effort group are in a single view, which means that the data was fully
				// aggregated.
				// - TruncateResultView can be used to rewind the cursor to the beginning.
				bool CanReturnMoreData() const {
					return !input_exhausted_ ||
						(result_.rows_remaining() > result_.row_count());
				}

				// Truncates the current result ViewIterator. If we only called Next() once,
				// this rewinds the Cursor to the beginning.
				bool TruncateResultView() {
					return result_.truncate(0);
				}

				virtual bool IsWaitingOnBarrierSupported() const {
					return child_.is_waiting_on_barrier_supported();
				}

				virtual void Interrupt() { child_.Interrupt(); }

				virtual void ApplyToChildren(CursorTransformer* transformer) {
					child_.ApplyToCursor(transformer);
				}

				virtual CursorId GetCursorId() const {
					return best_effort_
						? BEST_EFFORT_GROUP_AGGREGATE
						: GROUP_AGGREGATE;
				}

				const TupleSchema& group_schema() const {
					return group_data_block_.schema();
				}

				const BufferAllocator* allocator() {
					return allocator_.get();
				}

				BufferAllocator* group_data_block_allocator() {
					return group_data_block_allocator_;
				}

				BufferAllocator* vector_allocator() {
					return vector_allocator_;
				}

				BufferAllocator* not_const_allocator() {
					return not_const_allocator_;
				}

				const BufferAllocator* original_allocator() {
					return original_allocator_;
				}

				const size_t current_row_group_id() const {
					return current_row_group_id_;
				}

				void set_current_row_group_id(size_t new_row_group_id) {
					current_row_group_id_ = new_row_group_id;
				}

				void inc_current_row_group_id() {
					current_row_group_id_ ++;
				}

				CursorIterator& fact_table_cursor_iterator() {
					return child_;
				}

				pthread_mutex_t* fact_table_cursor_iterator_lock() {
					return &fact_table_cursor_iterator_lock_;
				}

				pthread_mutex_t* group_key_set_lock() {
					return &group_key_set_lock_;
				}

				pthread_mutex_t* aggregator_lock() {
					return &aggregator_lock_;
				}

				pthread_mutex_t* aggregator_reallocate_lock() {
					return &aggregator_reallocate_lock_;
				}

				void thread_views_push(View* view) {
					pthread_mutex_lock(&thread_views_lock_);
					thread_views_.push_back(view);
					pthread_mutex_unlock(&thread_views_lock_);
				}

				const vector<int>* filter_order(size_t row_group_id) {
					return &(filter_order_->at(row_group_id));
				}

				GroupKeySet* group_key_set() {
					return key_.get();
				}

				Aggregator* aggregator() {
					return aggregator_.get();
				}

				const int64 max_unique_keys_in_result() {
					return max_unique_keys_in_result_;
				}

				bool reset_aggregator_in_processinput() {
					return reset_aggregator_in_processinput_;
				} 

				void set_input_exhausted(bool input_exhausted) {
					input_exhausted_ = input_exhausted;
				} 	

				const TupleSchema& result_schema() {
					return result_.schema();
				}

				const BoundMultiSourceProjector* result_projector() {
					return result_projector_.get();
				}

				// Format the row_id from other formats to rowid_t.
				rowid_t FormatRowId(const Column& column, rowid_t row_id, bool from_column_piece); 

				// Update the selection vector by the filter order.
				FailureOrVoid UpdateSelectionVector(const View& view, rowcount_t row_count, const vector<int>* filter_order, bool_ptr selection_vector);

				// Translate the row_id key view to raw data view.
				FailureOrVoid TranslateKeyToRawData(const View& view, rowcount_t row_count, bool_ptr selection_vector, rowcount_t insert_row_count, Block& group_data_block);

			private:
				// Takes ownership of the allocator, key, aggregator, and child.
				MeasureAggregateCursor(const TupleSchema& result_schema,
						BufferAllocator* allocator,
						BufferAllocator* original_allocator,
						const vector<vector<int>>* filter_order,
						vector<const View*> group_vectors,
						const BoundSingleSourceProjector* group_by,
						vector<int> group_id,
						GroupKeySet* key,
						const TupleSchema& group_schema,
						Aggregator* aggregator,
						const BoundMultiSourceProjector* result_projector,
						bool best_effort,
						const int64 max_unique_keys_in_result,
						Cursor* child)
					: BasicCursor(result_schema),
					allocator_(allocator),
					original_allocator_(CHECK_NOTNULL(original_allocator)),
					group_data_block_allocator_(allocator),
					vector_allocator_(allocator),
					not_const_allocator_(allocator),
					filter_order_(filter_order),
					group_vectors_(group_vectors),
					group_by_(group_by),
					group_id_(group_id),
					child_(child),
					key_(key),
					group_data_block_(group_schema, allocator),
					aggregator_(aggregator),
					thread_views_(),
					result_(result_schema),
					result_projector_(result_projector),
					inserted_keys_(/*Cursor::kDefaultRowCount*/Optimizer::kRowGroupSize),
					best_effort_(best_effort),
					input_exhausted_(false),
					reset_aggregator_in_processinput_(false),
					max_unique_keys_in_result_(max_unique_keys_in_result),
					current_row_group_id_(0) {
						pthread_mutex_init(&fact_table_cursor_iterator_lock_, NULL);
						pthread_mutex_init(&group_key_set_lock_, NULL);
						pthread_mutex_init(&aggregator_lock_, NULL);
						pthread_mutex_init(&aggregator_reallocate_lock_, NULL);
						pthread_mutex_init(&thread_views_lock_, NULL);
					}

				// Process as many rows from input as can fit into result block. If after the
				// first call to ProcessInput() input_exhausted_ is true, the result is fully
				// aggregated (there are no rows with equal group by columns).
				// Initializes the result_ to iterate over the aggregation result.
				FailureOrVoid ProcessInput();

				// Owned allocator used to allocate the memory.
				// NOTE: it is used by other member objects created by GroupAggregateCursor so
				// it has to be destroyed last. Keep it as the first class member.
				std::unique_ptr<const BufferAllocator> allocator_;
				// Non-owned allocator used to check whether we can allocate more memory or
				// not.
				const BufferAllocator* original_allocator_;
				BufferAllocator* group_data_block_allocator_;
				BufferAllocator* vector_allocator_;
				BufferAllocator* not_const_allocator_;

				// The filter order of the fact table.
				const vector<vector<int>>* filter_order_;

				// The Encoded Group Vectors of dimension tables.
				vector<const View*> group_vectors_;

				const BoundSingleSourceProjector* group_by_;
				vector<int> group_id_;
				// The input.
				CursorIterator child_;

				// Holds key columns of the result. Wrapper around RowHashSet.
				std::unique_ptr<GroupKeySet> key_;

				// Holds the corresponding raw data of the key.
				Block group_data_block_;

				// Holds 'aggregated' columns of the result.
				std::unique_ptr<Aggregator> aggregator_;

				// Holds every thread's result
				vector<View*> thread_views_;

				// Iterates over a result of last call to ProcessInput. If
				// cursor_over_result_->Next() returns EOS and input_exhausted() is false,
				// ProcessInput needs to be called again to prepare next part of a result and
				// set cursor_over_result_ to iterate over it.
				ViewIterator result_;

				// Projector to combine key & aggregated columns into the result.
				std::unique_ptr<const BoundMultiSourceProjector> result_projector_;

				GroupKeySet::FindResult inserted_keys_;

				// If true, OOM is not fatal; the data aggregated up-to OOM are emitted,
				// and the aggregation starts anew.
				bool best_effort_;

				// Set when EOS reached in the input stream.
				bool input_exhausted_;

				// To track when ProcessInput() should reset key_ and aggregator_. It
				// shouldn't be done after exiting with WAITING_ON_BARRIER - some data might
				// lost. Reset is also not needed in first ProcessInput() call.
				bool reset_aggregator_in_processinput_;

				// Maximum number of unique key combination(as per input order) to aggregate
				// the results upon. If limit is hit, all remaining rows are aggregated
				// together in the last row at index = max_unique_keys_in_result_
				const int64 max_unique_keys_in_result_;

				size_t current_row_group_id_;

				pthread_mutex_t fact_table_cursor_iterator_lock_;

				pthread_mutex_t group_key_set_lock_;

				pthread_mutex_t aggregator_lock_;

				pthread_mutex_t aggregator_reallocate_lock_;

				pthread_mutex_t thread_views_lock_;

				DISALLOW_COPY_AND_ASSIGN(MeasureAggregateCursor);
		};

		inline rowid_t MeasureAggregateCursor::FormatRowId(const Column& column, rowid_t row_id, bool from_column_piece) {
			rowid_t dimension_row_id = 0;
			string temp;
			//stringstream ss;
			if(from_column_piece) {
				switch(column.type_info().type()) {
					case INT32:
						dimension_row_id = *(column.data_plus_offset_through_column_piece(row_id).as<INT32>());
						break;
					case INT64:
						dimension_row_id = *(column.data_plus_offset_through_column_piece(row_id).as<INT64>());
						break;
						/*
						   case STRING:
						   temp = column.data_plus_offset_through_column_piece(row_id).as_variable_length()->as_string();
						   ss << temp;
						   ss >> dimension_row_id;
						   break;
						   */
					default:
						break;
				}
			}

			else {
				switch(column.type_info().type()) {
					case INT32:
						dimension_row_id = *(column.data_plus_offset(row_id).as<INT32>());
						break;
					case INT64:
						dimension_row_id = *(column.data_plus_offset(row_id).as<INT64>());
						break;
						/*
						   case STRING:
						   temp = column.data_plus_offset(row_id).as_variable_length()->as_string();
						   ss << temp;
						   ss >> dimension_row_id;
						   break;
						   */
					default:
						break;
				}
			}
			return dimension_row_id;
		}

		FailureOrVoid MeasureAggregateCursor::UpdateSelectionVector(const View& view, 
				rowcount_t row_count,
				const vector<int>* filter_order,
				bool_ptr selection_vector) {
			//int count = 0;
			//for(int i = 0; i < 100000000; i++) { count++; }
			for(size_t column_id = 0; column_id < filter_order->size(); column_id++) {
				// The column of the fact table.
				// TODO(axyu) I can replace the dimension_row_id in the fact table with the
				// new group vector row_id.
				const Column& fact_column = view.column(group_by_->source_attribute_position(filter_order->at(column_id)));
				//const int64* fact_row_ptr = fact_column.typed_data<INT64>();
				// The column of the dimension table.
				const Column& dimension_column = group_vectors_[filter_order->at(column_id)]->column(0);
				bool_const_ptr is_null = dimension_column.is_null();
				for(size_t row_id = 0; row_id < row_count; row_id++) {
					rowid_t dimension_row_id = 0;
					if(!selection_vector[row_id]) continue;
					else {
						//dimension_row_id = *(fact_column.data_plus_offset_through_column_piece(row_id).as<INT64>());
						dimension_row_id = FormatRowId(fact_column, row_id, true);
						//if(is_null != NULL && is_null[fact_row_ptr[row_id]]) selection_vector[row_id] = false;
						//if(is_null != NULL && is_null[*(fact_column.data_plus_offset_through_column_piece(row_id).as<INT64>())])
						if(is_null != NULL && is_null[dimension_row_id])
							selection_vector[row_id] = false;
					}
				}
			}
			return Success();
		}

		FailureOrVoid MeasureAggregateCursor::TranslateKeyToRawData(const View& view, rowcount_t row_count, bool_ptr selection_vector, rowcount_t insert_row_count, Block& group_data_block) {
			rowid_t* row_id = new rowid_t[insert_row_count];
			for(size_t column_id = 0; column_id < group_id_.size(); column_id++) {
				ColumnCopier column_copier(ResolveCopyColumnFunction(group_data_block.schema().attribute(column_id).type(), 
							group_vectors_[group_id_[column_id]]->schema().attribute(0).nullability(),
							group_data_block.schema().attribute(column_id).nullability(),
							INPUT_SELECTOR,
							true));
				rowid_t insert_id = 0;
				for(rowcount_t id = 0; id < row_count; id++) {
					if(selection_vector[id]) {
						row_id[insert_id++] = FormatRowId(view.column(group_by_->source_attribute_position(group_id_[column_id])), id, true);
						// row_id[insert_id++] = *(view.column(group_by_->source_attribute_position(group_id_[column_id])).data_plus_offset_through_column_piece(id).as<INT64>());
					}
				}
				//const rowid_t* row_id = key_view.column(column_id).typed_data<INT64>();
				group_data_block.mutable_column(column_id)->set_row_count(0); // set row count
				column_copier(insert_row_count, group_vectors_[group_id_[column_id]]->column(0), row_id, 0, group_data_block.mutable_column(column_id));
			}
			return Success();
		}


		FailureOrVoid MeasureAggregateCursor::ProcessInput() {
			if (reset_aggregator_in_processinput_) {
				reset_aggregator_in_processinput_ = false;
				key_->Reset();
				// Compacting GroupKeySet to release more memory. This is a workaround for
				// having (allocator_->Available() == 0) constantly, which would allow to
				// only process one block of input data per call to ProcessInput(). However,
				// freeing the underlying datastructures and building them from scratch many
				// times can be inefficient.
				// TODO(user): Implement a less aggressive solution to this problem.
				key_->Compact();
				aggregator_->Reset();
			}
			vector<pthread_t> id(Optimizer::kThreadCount, -1);
			for(int i = 0; i < Optimizer::kThreadCount; i++) {
				int ret = pthread_create(&id[i], NULL, thread, (void*)this);
				CHECK_EQ(ret, 0) << "Thread id = " << i << " create error!" << std::endl;
			}
			for(int i = 0; i < Optimizer::kThreadCount; i++) {
				pthread_join(id[i], NULL);
			}

			/*
			   Merage the result of threads
			   */
			Aggregator* aggregator = Aggregator::Create(
					AggregationSpecification::CreateSameOutputNameAS(aggregator_->aggregation_specification()),
					result_.schema(),
					not_const_allocator_,
					aggregator_->result_initial_row_capacity()).release();

			CHECK_EQ(thread_views_.size(), Optimizer::kThreadCount) << " thread_views.size() != Optimizer::kThreadCount" << std::endl;
			rowcount_t row_count = key_->size();
			for(int thread_id = 0; thread_id < thread_views_.size(); thread_id++) {
				key_->Insert(*(thread_views_[thread_id]), &inserted_keys_);
				row_count = key_->size();
				std::cout << "row count: " << row_count << std::endl;
				if (aggregator->capacity() < row_count) {
					// Not enough space to hold the aggregate columns; need to reallocate.
					// But, if already over quota, give up.
					if (allocator_->Available() == 0) {
						// Rewind the input and ignore trailing rows.
						row_count = aggregator->capacity();
						for (rowid_t i = 0; i < thread_views_[thread_id]->row_count(); ++i) {
							if (inserted_keys_.row_ids()[i] >= row_count) {
								//child_.truncate(i);
								std::cout << "aggregate_groups.cc:670 child_truncate() hit" << std::endl;
								break;
							}
						}
					} else {
						if (original_allocator_->Available() < allocator_->Available()) {
							THROW(new Exception(ERROR_MEMORY_EXCEEDED,
										"Underlying allocator ran out of memory."));
						}
						// Still have spare memory; reallocate.
						rowcount_t requested_capacity = std::max(2 * aggregator->capacity(),
								row_count);
						if (!aggregator->Reallocate(requested_capacity)) {
							// OOM when trying to make more room for aggregate columns. Rewind the
							// last input block, so that it is re-fetched by the next
							// ProcessInput, and break out of the loop, returning what we have up
							// to now.
							row_count = aggregator->capacity();
							// child_.truncate(0);
							std::cout << "aggregate_groups.cc:689 child_truncate() hit" << std::endl;
							break;
						}
					}
				}
				// Update the aggregate columns w/ new rows.
				PROPAGATE_ON_FAILURE(
						aggregator->UpdateAggregations(*(thread_views_[thread_id]),
							inserted_keys_.row_ids()));
			}
			row_count = key_->size();
			if (!input_exhausted_) {
				if (best_effort_) {
					if (row_count == 0) {
						THROW(new Exception(
									ERROR_MEMORY_EXCEEDED,
									StringPrintf(
										"In best-effort mode, failed to process even a single row. "
										"Memory free: %zd",
										allocator_->Available())));
					}
				} else {  // Non-best-effort.
					THROW(new Exception(
								ERROR_MEMORY_EXCEEDED,
								StringPrintf(
									"Failed to process the entire input. "
									"Memory free: %zd",
									allocator_->Available())));
				}
			}
			const View* views[] = { &key_->key_view(), &aggregator->data() };
			result_projector_->Project(&views[0], &views[2], my_view());
			my_view()->set_row_count(row_count);
			result_.reset(*my_view());
			reset_aggregator_in_processinput_ = true;
			return Success();
		}

		class MeasureAggregateOperation : public BasicOperation {
			public:
				// Takes ownership of SingleSourceProjector, AggregationSpecification and
				// child_operation.
				MeasureAggregateOperation(const SingleSourceProjector* group_by,
						vector<int> group_id,
						const AggregationSpecification* aggregation,
						GroupAggregateOptions* options,
						bool best_effort,
						const vector<vector<int>>* filter_order,
						vector<Operation*> children)
					: BasicOperation(children),
					group_by_(group_by),
					group_id_(group_id),
					aggregation_specification_(aggregation),
					best_effort_(best_effort),
					options_(options != NULL ? options : new GroupAggregateOptions()),
					filter_order_(filter_order) {}

				virtual ~MeasureAggregateOperation() {}

				virtual FailureOrOwned<Cursor> CreateCursor() const {
					vector<Cursor*> lhs_children_cursors;
					for(size_t childId = 0; childId < children_count() - 1; childId++) {
						lhs_children_cursors.push_back(child_at(childId)->CreateCursor().release());
					}
					vector<const View*> group_vectors;
					// Get the edncoded group vectors from all dimension tables.
					for(size_t childId = 0; childId < lhs_children_cursors.size(); childId++) {
						Block* result_block = new Block(lhs_children_cursors[childId]->schema(), buffer_allocator());
						PROPAGATE_ON_FAILURE(FetchEncodedGroupVector(lhs_children_cursors[childId], result_block));
						group_vectors.push_back(&result_block->view());
					}

					FailureOrOwned<Cursor> rhs_child_cursor = child_at(children_count() - 1)->CreateCursor();
					PROPAGATE_ON_FAILURE(rhs_child_cursor);

					BufferAllocator* original_allocator = buffer_allocator();
					std::unique_ptr<BufferAllocator> allocator;
					if (options_->enforce_quota()) {
						allocator.reset(new GuaranteeMemory(options_->memory_quota(),
									original_allocator));
					} else {
						allocator.reset(new MemoryLimit(
									options_->memory_quota(), false, original_allocator));
					}
					FailureOrOwned<Aggregator> aggregator = Aggregator::Create(
							*aggregation_specification_, rhs_child_cursor->schema(),
							allocator.get(),
							options_->estimated_result_row_count());
					PROPAGATE_ON_FAILURE(aggregator);
					FailureOrOwned<const BoundSingleSourceProjector> bound_group_by =
						group_by_->Bind(rhs_child_cursor->schema());
					PROPAGATE_ON_FAILURE(bound_group_by);
					return BoundGroupAggregateWithLimit(
							bound_group_by.release(), 
							group_id_,
							aggregator.release(),
							allocator.release(),
							original_allocator,
							best_effort_,
							options_->max_unique_keys_in_result(),
							filter_order_, 
							group_vectors,
							rhs_child_cursor.release());
				}

			private:
				// Get the all group vector data from the children cursors.
				FailureOrVoid FetchEncodedGroupVector(Cursor* source_cursor, Block* result_block) const {
					ResultView result = ResultView::EOS();
					ViewCopier view_copier(source_cursor->schema(), false);
					rowcount_t write_ptr = 0;
					while((result = source_cursor->Next(Optimizer::kRowGroupSize)).has_data()) {
						if(!result_block->Reallocate(result_block->view().row_count() + result.view().row_count())) {
							THROW(new Exception(ERROR_MEMORY_EXCEEDED,
										"Couldn't allocate block for group vector's result block"));
						}
						rowcount_t rows_copied = view_copier.Copy(result.view().row_count(),
								result.view(),
								write_ptr,
								result_block);
						write_ptr += rows_copied;
					}
					return Success();
				};

				std::unique_ptr<const SingleSourceProjector> group_by_;
				vector<int> group_id_;
				std::unique_ptr<const AggregationSpecification> aggregation_specification_;
				const bool best_effort_;
				std::unique_ptr<GroupAggregateOptions> options_;
				const vector<vector<int>>* filter_order_;
				DISALLOW_COPY_AND_ASSIGN(MeasureAggregateOperation);
		};

		void* thread(void* arg) {
			std::cout << "Thread id : " << pthread_self() << std::endl;
			MeasureAggregateCursor* measure_aggregate_cursor = 
				(MeasureAggregateCursor*)arg;
			Block group_data_block(measure_aggregate_cursor->group_schema(), measure_aggregate_cursor->group_data_block_allocator());
			if(!group_data_block.Reallocate(Optimizer::kRowGroupSize)) {
				//	THROW(new Exception(ERROR_MEMORY_EXCEEDED,
				//				"Couldn't allocate block for group raw data's block"));
				std::cerr << "Couldn't allocate block for group raw data's block" << std::endl;
			}

			bool_array* selection_vector = new bool_array();

			CursorIterator& child_ = measure_aggregate_cursor->fact_table_cursor_iterator();

			std::unique_ptr<const SingleSourceProjector> key_projector(ProjectAllAttributes());
			FailureOrOwned<const BoundSingleSourceProjector> bound_key_projector(
					key_projector->Bind(group_data_block.schema()));

			Aggregator* aggregator = Aggregator::Create(
					measure_aggregate_cursor->aggregator()->aggregation_specification(), child_.schema(),
					measure_aggregate_cursor->not_const_allocator(),
					measure_aggregate_cursor->aggregator()->result_initial_row_capacity()).release();

			GroupKeySet* key = GroupKeySet::Create(
					bound_key_projector.release(), measure_aggregate_cursor->not_const_allocator(), aggregator->capacity(),
					measure_aggregate_cursor->max_unique_keys_in_result()).release();

			//size_t row_group_id = 0;

			// Process the input while not exhausted and memory quota not exceeded.
			// (But, test the condition at the end of the loop, to guarantee that we
			// process at least one chunk of the input data).
			int counter = 0;
			do {
				counter++;
				pthread_mutex_lock(measure_aggregate_cursor->fact_table_cursor_iterator_lock());
				// Fetch next block from the input.
				if (!PREDICT_TRUE(child_.Next(Optimizer::kRowGroupSize, false))) {
					//PROPAGATE_ON_FAILURE(child_);
					if (!child_.has_data()) {
						if (child_.is_eos()) {
							measure_aggregate_cursor->set_input_exhausted(true);
						} else {
							DCHECK(child_.is_waiting_on_barrier());
							DCHECK(!measure_aggregate_cursor->reset_aggregator_in_processinput());
							pthread_mutex_unlock(measure_aggregate_cursor->fact_table_cursor_iterator_lock());
							return NULL;//Success();
						}
					}
					pthread_mutex_unlock(measure_aggregate_cursor->fact_table_cursor_iterator_lock());
					break;
				}
				View child_view(child_.view(), child_.view().row_count());
				size_t row_group_id = measure_aggregate_cursor->current_row_group_id();
				measure_aggregate_cursor->inc_current_row_group_id();
				pthread_mutex_unlock(measure_aggregate_cursor->fact_table_cursor_iterator_lock());
				//std::cout << "child row count: " << child_.view().row_count() << std::endl;
				//ViewPrinter view_printer;
				//std::cout << ">>>>child start<<<<<" << std::endl;
				//view_printer.AppendViewToStream(child_.view(), &std::cout);
				//std::cout << ">>>>child end<<<<<" << std::endl;

				group_data_block.ResetArenas();

				// Calculate the selection vector of the child_'s rows.
				rowcount_t child_row_count = child_view.row_count();
				selection_vector->Reallocate(child_row_count, measure_aggregate_cursor->vector_allocator());
				for(rowcount_t i = 0; i < child_row_count; i++) {
					*(selection_vector->mutable_data() + i) = true;
				}
				// Update the selection vector by the given fact table's filter order.
				measure_aggregate_cursor->UpdateSelectionVector(child_view, child_row_count, measure_aggregate_cursor->filter_order(row_group_id), selection_vector->mutable_data());

				rowcount_t insert_row_count = 0;
				for(int sv_id = 0; sv_id < child_row_count; sv_id++) {
					if(selection_vector->const_data()[sv_id]) insert_row_count++;
				}
				rowid_t* row_id = new rowid_t[insert_row_count];
				int row_id_id = 0;
				for(int sv_id = 0; sv_id < child_row_count; sv_id++) {
					if(selection_vector->const_data()[sv_id]) row_id[row_id_id++] = sv_id;
				}
				// Translate the row id key to raw data in the group vector.
				measure_aggregate_cursor->TranslateKeyToRawData(child_view, child_row_count, selection_vector->mutable_data(), insert_row_count, group_data_block);
				View key_view(group_data_block.schema());
				key_view.ResetFromSubRange(group_data_block.view(), 0, insert_row_count);
				// Add new rows to the key set.
				GroupKeySet::FindResult inserted_keys_(Optimizer::kRowGroupSize);
				//pthread_mutex_lock(measure_aggregate_cursor->group_key_set_lock());
				rowcount_t inserted = key->Insert(key_view, &inserted_keys_);
				//pthread_mutex_unlock(measure_aggregate_cursor->group_key_set_lock());
				//child_.truncate((inserted == insert_row_count) ? child_row_count : row_id[inserted - 1]);
				CHECK_EQ(inserted, insert_row_count) << "inserted != insert_row_count: " << inserted << " != " << insert_row_count;
				child_.truncate(child_row_count);
				if (child_view.row_count() == 0) {
					// Failed to add any new keys.
					break;
				}

				//pthread_mutex_lock(measure_aggregate_cursor->aggregator_reallocate_lock());
				rowcount_t row_count = key->size();
				if (aggregator->capacity() < row_count) {
					// Not enough space to hold the aggregate columns; need to reallocate.
					// But, if already over quota, give up.
					if (measure_aggregate_cursor->allocator()->Available() == 0) {
						// Rewind the input and ignore trailing rows.
						row_count = aggregator->capacity();
						for (rowid_t i = 0; i < key_view.row_count(); ++i) {
							if (inserted_keys_.row_ids()[i] >= row_count) {
								child_.truncate(row_id[i]);
								break;
							}
						}
					} else {
						if (measure_aggregate_cursor->original_allocator()->Available() < measure_aggregate_cursor->allocator()->Available()) {
							//THROW(new Exception(ERROR_MEMORY_EXCEEDED,
							//			"Underlying allocator ran out of memory."));
							std::cerr << "Underlying allocator ran out of memory." << std::endl;
						}
						// Still have spare memory; reallocate.
						rowcount_t requested_capacity = std::max(2 * aggregator->capacity(),
								row_count);
						if (!aggregator->Reallocate(requested_capacity)) {
							// OOM when trying to make more room for aggregate columns. Rewind the
							// last input block, so that it is re-fetched by the next
							// ProcessInput, and break out of the loop, returning what we have up
							// to now.
							row_count = aggregator->capacity();
							child_.truncate(0);
							break;
						}
					}
				}
				//pthread_mutex_unlock(measure_aggregate_cursor->aggregator_reallocate_lock());

				vector<rowid_t> selected_inputs_indexes;
				for(rowid_t i = 0; i < child_row_count; i++) {
					if(*(selection_vector->mutable_data() + i))
						selected_inputs_indexes.push_back(i);
				}

				rowid_t* insert_row_id = new rowid_t[child_row_count];
				const rowid_t* inserted_keys_row_ids = inserted_keys_.row_ids();
				for(int i = 0; i < child_row_count; i++) insert_row_id[i] = -1;
				for(int i = 0; i < insert_row_count; i++) insert_row_id[row_id[i]] = inserted_keys_row_ids[i];
				// Update the aggregate columns w/ selective new rows.
				//pthread_mutex_lock(measure_aggregate_cursor->aggregator_lock());
				aggregator->UpdateSelectionAggregations(child_view,
						insert_row_id,
						selected_inputs_indexes);
				//pthread_mutex_unlock(measure_aggregate_cursor->aggregator_lock());
				delete[] row_id;
				delete[] insert_row_id;
			} while (measure_aggregate_cursor->allocator()->Available() > 0);

			rowcount_t row_count = key->size();
			View* view = new View(measure_aggregate_cursor->result_schema());
			const View* views[] = { &key->key_view(), &aggregator->data() };
			measure_aggregate_cursor->result_projector()->Project(&views[0], &views[2], view);
			view->set_row_count(row_count);
			measure_aggregate_cursor->thread_views_push(view);
			std::cout << "Thread id : " << pthread_self() << " does " << counter << " times." << std::endl;
		}
	}  // namespace


	// Operation
	Operation* MeasureAggregate(
			const SingleSourceProjector* group_by,
			vector<int> group_id,
			const AggregationSpecification* aggregation,
			GroupAggregateOptions* options,
			const vector<vector<int>>* filter_order,
			vector<Operation*> lhs_children,
			Operation* rhs_child) {
		lhs_children.push_back(rhs_child);
		return new MeasureAggregateOperation(group_by, group_id, aggregation, options, false, filter_order, lhs_children);
	}

	Operation* BestEffortGroupAggregate(
			const SingleSourceProjector* group_by,
			vector<int> group_id,
			const AggregationSpecification* aggregation,
			GroupAggregateOptions* options,
			const vector<vector<int>>* filter_order,
			vector<Operation*> lhs_children,
			Operation* rhs_child) {
		lhs_children.push_back(rhs_child);
		return new MeasureAggregateOperation(group_by, group_id, aggregation, options, true, filter_order, lhs_children);
	}


	// Cursor
	// TODO(user): Remove this variant in favor of BoundGroupAggregateWithLimit
	FailureOrOwned<Cursor> BoundGroupAggregate(
			const BoundSingleSourceProjector* group_by,
			vector<int> group_id,
			Aggregator* aggregator,
			BufferAllocator* allocator,
			BufferAllocator* original_allocator,
			bool best_effort,
			const vector<vector<int>>* filter_order,
			vector<const View*> group_vectors,
			Cursor* child) {
		return BoundGroupAggregateWithLimit(group_by,
				group_id,
				aggregator,
				allocator,
				original_allocator,
				best_effort,
				kint64max,
				filter_order,
				group_vectors,
				child);
	}

	FailureOrOwned<Cursor> BoundGroupAggregateWithLimit(
			const BoundSingleSourceProjector* group_by,
			vector<int> group_id,
			Aggregator* aggregator,
			BufferAllocator* allocator,
			BufferAllocator* original_allocator,
			bool best_effort,
			const int64 max_unique_keys_in_result,
			const vector<vector<int>>* filter_order,
			vector<const View*> group_vectors,
			Cursor* child) {
		FailureOrOwned<MeasureAggregateCursor> result =
			MeasureAggregateCursor::Create(
					group_by, group_id, aggregator, allocator,
					original_allocator == NULL ? allocator : original_allocator,
					best_effort, max_unique_keys_in_result, filter_order, group_vectors, child);
		PROPAGATE_ON_FAILURE(result);
		return Success(result.release());
	}

}  // namespace supersonic
