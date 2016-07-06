#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_TEST_DATA_GENERATER_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_TEST_DATA_GENERATER_H_
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/projector.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/cursor/infrastructure/table.h"
#include "supersonic/utils/file.h"

#include <iostream>
using std::cout;
using std::endl;
#include<vector>
using std::vector;
#include <sstream>
using std::stringstream;
#include <string>
using std::string;
#include<stdlib.h>
namespace supersonic {

	class TestDataSet {
		public:
			TestDataSet(const TupleSchema& fact_schema,
					const vector<TupleSchema>& dimension_schemas,
					const string fact_table_name,
					const vector<vector<string>> dimension_tables_names,
					const rowcount_t fact_table_rows_count,
					const int fact_table_column_count,
					const rowcount_t dimension_table_rows_count)
				: fact_schema_(fact_schema),
				dimension_schemas_(dimension_schemas),
				fact_table_name_(fact_table_name),
				dimension_tables_names_(dimension_tables_names),
				fact_table_rows_count_(fact_table_rows_count),
				fact_table_column_count_(fact_table_column_count),
				dimension_table_rows_count_(dimension_table_rows_count) {}

			const View& FactView(const vector<vector<bool>>& storage_map) const {
				File *fp = File::OpenOrDie("../storage/" + fact_table_name_, "r");
				FailureOrOwned<Cursor> cu(FileInput(fact_schema_, fp, false,
							HeapBufferAllocator::Get()));

				Table* table = new Table(fact_schema_, HeapBufferAllocator::Get());
				vector<vector<StorageType>> storage_type_vector(storage_map.size(),vector<StorageType>());

				vector<rowcount_t> in_memory_row_capacity_vector(fact_table_column_count_, 0);
				for(int map_row_index = 0; map_row_index < storage_map.size(); map_row_index++) {
					for(int map_column_index = 0; map_column_index < storage_map[map_row_index].size(); map_column_index++) {
						if(storage_map[map_row_index][map_column_index]) {
							in_memory_row_capacity_vector[map_column_index] += rowGroupSize;
							storage_type_vector[map_row_index].push_back(MEMORY);
						} else {
							storage_type_vector[map_row_index].push_back(DISK);
						}
					}
					storage_type_vector[map_row_index].push_back(DISK);
				}

				table->ReserveRowCapacityOneTime(this->fact_table_rows_count_, in_memory_row_capacity_vector);
				int storage_type_row_index = 0;
				while(1) {
					ResultView result(cu->Next(rowGroupSize));
					if(result.has_data())
					{
						table->AppendView(result.view(), storage_type_vector[storage_type_row_index]);
						storage_type_row_index ++;
					} else {
						break;
					}
				}
				return table->view();
			}
			const View& DimensionViews(int query_id, int key_id) const{
				string table_name = "../storage/" + dimension_tables_names_[query_id][key_id];
				File *fp = File::OpenOrDie(table_name, "r");
				FailureOrOwned<Cursor> cu(FileInput(dimension_schemas_[key_id], fp, false,
							HeapBufferAllocator::Get()));

				Table* table = new Table(dimension_schemas_[key_id], HeapBufferAllocator::Get());
				vector<StorageType> storage_type(dimension_schemas_[key_id].attribute_count(), MEMORY);
				vector<rowcount_t> in_memory_row_capacity_vector(1, this->dimension_table_rows_count_);
				table->ReserveRowCapacityOneTime(this->dimension_table_rows_count_, in_memory_row_capacity_vector);

				while(1) {
					ResultView result(cu->Next(rowGroupSize));
					if(result.has_data())
					{
						table->AppendView(result.view(), storage_type);
					} else {
						break;
					}
				}
				return table->view();
			}

		private:
			//	const BoundSingleSourceProjector* key_projector_;
			const TupleSchema& fact_schema_;
			const vector<TupleSchema>& dimension_schemas_;
			const string fact_table_name_;
			const vector<vector<string>> dimension_tables_names_;
			const rowcount_t fact_table_rows_count_;
			const int fact_table_column_count_;
			const rowcount_t dimension_table_rows_count_;
	};

	class TestDataGenerater {
		public:
			static const std::string storageDirectory;
			static const rowcount_t dimensionRowsCount = 1024;

			TestDataGenerater(const TupleSchema& fact_schema,
					const vector<TupleSchema>& dimension_schemas,
					rowcount_t row_group_size,
					rowcount_t row_group_count,
					int query_count,
					int fact_column_count,
					int key_column_count,
					const SingleSourceProjector* key_projector,
					const vector<int>* column_width,
					const vector<vector<double>>* visit_locality,
					const vector<vector<double>>* selectivity)
				:fact_schema_(fact_schema),
				dimension_schemas_(dimension_schemas),
				row_group_size_(row_group_size),
				row_group_count_(row_group_count),
				query_count_(query_count),
				fact_column_count_(fact_column_count),
				key_column_count_(key_column_count),
				column_width_(column_width),
				visit_locality_(visit_locality),
				selectivity_(selectivity),
				fact_table_(new Table(fact_schema, HeapBufferAllocator::Get())),
				dimension_tables_(query_count, vector<Table*>()){
					CHECK_EQ(visit_locality->size(), key_column_count);
					CHECK_EQ(column_width->size(), fact_column_count);
					CHECK_EQ(selectivity->size(), query_count);
					CHECK_EQ(dimension_schemas.size(), key_column_count);

					FailureOrOwned<const BoundSingleSourceProjector> bound_key_projector =
						key_projector->Bind(fact_schema);
					//PROPAGATE_ON_FAILURE(bound_key_projector);
					key_projector_ = bound_key_projector.release();
					for(int query_id = 0; query_id < query_count; query_id++){
						for(int dimension_id = 0; dimension_id < key_column_count; dimension_id++) {
							dimension_tables_[query_id].push_back(new Table(dimension_schemas[dimension_id], HeapBufferAllocator::Get()));
						}
					}
				}

			void GenerateDimensionTable(){
				for(int query_id = 0; query_id < query_count_; query_id++){
					for(int dimension_id = 0; dimension_id < key_column_count_; dimension_id++) {
						for(rowid_t row_id = 0; row_id < dimensionRowsCount; row_id++) {
							dimension_tables_[query_id][dimension_id]->AddRow();
							switch(dimension_schemas_[dimension_id].attribute(0).type()) {
								case INT32:
									dimension_tables_[query_id][dimension_id]->Set<INT32>(0, row_id, row_id);
									break;
								case INT64:
									dimension_tables_[query_id][dimension_id]->Set<INT64>(0, row_id, row_id);
									break;
								case DOUBLE:
									dimension_tables_[query_id][dimension_id]->Set<DOUBLE>(0, row_id, row_id);
									break;
								default :
									break;
							}
						}
					}
				}
			}

			string GenerateString(int width, rowcount_t s){
				stringstream ss;
				ss << s;
				string tmp(ss.str());
				while(tmp.length() < width - 16){
					tmp = " " + tmp;
				}
				return tmp;
			}

			void DataAllocator(rowcount_t row_count, const vector<double>& selectivity, vector<rowcount_t>& insert_count_vector) {
				CHECK_EQ(selectivity.size(), insert_count_vector.size());
				rowcount_t accumulate_count = 0;
				for(int i = 0; i < selectivity.size() - 1; i++) {
					insert_count_vector[i] = (rowcount_t)(row_count * selectivity[i]);
					accumulate_count += insert_count_vector[i];
				}
				insert_count_vector[insert_count_vector.size() - 1] = row_count - accumulate_count;
			}

			void GenerateFactTable() {
				// allocate table capacity
				for(rowid_t row_id = 0; row_id < row_group_size_ * row_group_count_; row_id++) {
					fact_table_->AddRow();
				}
				//
				for(int column_id = 0; column_id < fact_column_count_; column_id++) {
					if(!this->key_projector_->IsAttributeProjected(column_id)) {
						InsertData(column_id, 0, row_group_count_ * row_group_size_, column_id);
						continue;
					}
					for(int rg_id = 0; rg_id < row_group_count_; rg_id++) {
						if(column_id == 0) {
							InsertData(column_id, rg_id * row_group_size_, 4000, 0);
							InsertData(column_id, rg_id * row_group_size_ + 4000, 6240, 1);
						}
						if(column_id == 1) {
							InsertData(column_id, rg_id * row_group_size_, 20, 0);
							InsertData(column_id, rg_id * row_group_size_ + 20, 3180, 1);
							InsertData(column_id, rg_id * row_group_size_ + 3200, 20, 0);
							InsertData(column_id, rg_id * row_group_size_ + 3220, 1980, 1);
							InsertData(column_id, rg_id * row_group_size_ + 5200, 60, 0);
							InsertData(column_id, rg_id * row_group_size_ + 5260, 4980, 1);
						}
						if(column_id == 2) {
							InsertData(column_id, rg_id * row_group_size_, 3200, 1);
							InsertData(column_id, rg_id * row_group_size_ + 3200, 2000, 0);
							InsertData(column_id, rg_id * row_group_size_ + 5200, 5040, 1);
						}
					}	
					for(rowcount_t dim_null = 1; dim_null < dimensionRowsCount; dim_null++) {
							dimension_tables_[0][column_id]->SetNull(0, dim_null);
					}
				}
			}

			void InsertData(int column_id, rowid_t row_id, rowcount_t row_count, rowcount_t insert_data) {
				if(row_count == 0) return;
				for(rowid_t row_it = row_id; row_it < row_id + row_count; row_it++) {
					if(fact_table_->view().column(column_id).type_info().is_variable_length()) {
						fact_table_->Set<STRING>(column_id, row_it,
								GenerateString(column_width_->at(column_id), insert_data));
					} else {
						if(fact_table_->view().column(column_id).type_info().type() == INT32) {
							fact_table_->Set<INT32>(column_id, row_it, insert_data);
						} else if(fact_table_->view().column(column_id).type_info().type() == INT64) {
							fact_table_->Set<INT64>(column_id, row_it, insert_data);
						}
					}
				}
			}

			TestDataSet* const GenerateDataSet() {
				this->GenerateDimensionTable();
				this->GenerateFactTable();
				string fact_table_name = "fact_table";
				File* fp=File::OpenOrDie(storageDirectory + fact_table_name, "w");
				scoped_ptr<Sink> sink(FileOutput(fp, TAKE_OWNERSHIP));
				sink->Write(fact_table_->view()).is_failure();
				sink->Finalize();
				vector<vector<string>> dimension_tables_names(dimension_tables_.size(),vector<string>());
				for(int query_index = 0; query_index < this->dimension_tables_.size(); query_index++) {
					for(int column_index = 0; column_index < dimension_tables_[query_index].size(); column_index++) {

						stringstream ss;
						ss << query_index << "_" << column_index;
						dimension_tables_names[query_index].push_back("dimension_table_" + ss.str());
						File* fp=File::OpenOrDie(storageDirectory + dimension_tables_names[query_index][column_index], "w");
						scoped_ptr<Sink> sink(FileOutput(fp, TAKE_OWNERSHIP));
						sink->Write(dimension_tables_[query_index][column_index]->view()).is_failure();
						sink->Finalize();
					}
				}

				TestDataSet* test_data_set =
					new TestDataSet(fact_schema_,
							dimension_schemas_,
							fact_table_name,
							dimension_tables_names,
							row_group_size_ * row_group_count_,
							fact_column_count_,
							dimensionRowsCount);
				return test_data_set;
			}
			void check(){

			}
		private:
			const TupleSchema& fact_schema_;
			const vector<TupleSchema>& dimension_schemas_;
			rowcount_t row_group_size_;
			rowcount_t row_group_count_;
			int query_count_;
			int fact_column_count_;
			int key_column_count_;
			const BoundSingleSourceProjector* key_projector_;
			const vector<int>* column_width_;
			const vector<vector<double>>* visit_locality_;
			const vector<vector<double>>* selectivity_;
			scoped_ptr<Table> fact_table_;
			vector<vector<Table*>> dimension_tables_;
	};

	const std::string TestDataGenerater::storageDirectory ="../storage/";
} // namespace supersonic

#endif /* SUPERSONIC_BASE_INFRASTRUCTURE_TEST_DATA_GENERATER_H_ */
