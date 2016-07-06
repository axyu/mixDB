#ifndef DEKE_INCLUDE_DEMO_DATA_H
#define DEKE_INCLUDE_DEMO_DATA_H

#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/optimizer.h"

namespace supersonic {

class DemoDataSet{
public:
	DemoDataSet(rowcount_t row_group_count) 
	: row_group_count_(row_group_count),
	  key_width({24, 56, 32}){
		TupleSchema fact_schema;
		fact_schema.add_attribute(Attribute("key_1",STRING,NOT_NULLABLE));
		fact_schema.add_attribute(Attribute("key_2",STRING,NOT_NULLABLE));
		fact_schema.add_attribute(Attribute("key_3",STRING,NOT_NULLABLE));
		fact_schema.add_attribute(Attribute("agg",INT32,NOT_NULLABLE));

		TupleSchema dimension1_schema, dimension2_schema, dimension3_schema;
		dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));
		dimension2_schema.add_attribute(Attribute("dimension_2",INT32,NULLABLE));
		dimension3_schema.add_attribute(Attribute("dimension_3",DOUBLE,NULLABLE));
		fact_table_.reset(new Table(fact_schema, HeapBufferAllocator::Get()));
		dim1_table_.reset(new Table(dimension1_schema,HeapBufferAllocator::Get()));
		dim2_table_.reset(new Table(dimension2_schema,HeapBufferAllocator::Get()));
		dim3_table_.reset(new Table(dimension3_schema,HeapBufferAllocator::Get()));
		Generate();
	}
	void Generate() {
		long align = rowGroupSize / 1024;
		for(rowcount_t i = 0; i < 1024; i++) {
			dim1_table_->AddRow();
			dim2_table_->AddRow();
			dim3_table_->AddRow();
			dim1_table_->Set<DOUBLE>(0,i,i*0.1);
			dim2_table_->Set<INT32>(0,i,i);
			dim3_table_->Set<DOUBLE>(0,i,i*0.01);
		}
		for(rowcount_t rg_id = 0; rg_id < row_group_count_; rg_id++) {
			for(rowcount_t j = 0; j < rowGroupSize; j++) {
				fact_table_->AddRow();
			}
			for(int co_id = 0; co_id < 3; co_id++) {
				if(co_id == 2) {
					InsertFactTable(co_id, rg_id * rowGroupSize, 400 * align, 0);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 400 * align, 624 * align, 1);	
				} else if(co_id == 1) {	
					InsertFactTable(co_id, rg_id * rowGroupSize, 2 * align, 0);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 2 * align, 318 * align, 1);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 320 * align, 2 * align, 0);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 322 * align, 198 * align, 1);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 520 * align, 6 * align, 0);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 526 * align, 498 * align, 1);	
				} else if(co_id == 0) {	
					InsertFactTable(co_id, rg_id * rowGroupSize, 320 * align, 1);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 320 * align, 200 * align, 0);	
					InsertFactTable(co_id, rg_id * rowGroupSize + 520 * align, 504 * align, 1);	
				}
			}
		}
		InsertFactTable(3, 0, row_group_count_ * rowGroupSize, 1);
		for(rowcount_t null_id = 1; null_id < 1024; null_id ++) {
			dim1_table_->SetNull(0,null_id);
			dim2_table_->SetNull(0,null_id);
			dim3_table_->SetNull(0,null_id);
		}
		fact_table_->RebuildColumnPieceVector();	
		dim1_table_->RebuildColumnPieceVector();
		dim2_table_->RebuildColumnPieceVector();
		dim3_table_->RebuildColumnPieceVector();

	}
	string GenerateString(int width, rowcount_t num) {
		stringstream ss;
		ss << num;
		string tmp(ss.str());
		while(tmp.length() < width - 16) {
			tmp = " " + tmp;
		}
		return tmp;
	}
	void InsertFactTable(int column_id, rowid_t row_id, rowcount_t row_count, rowcount_t data) {
		if(column_id == 3) {
			for(rowid_t id = row_id; id < row_id + row_count; id++) {
				fact_table_->Set<INT32>(column_id, id, data);
			}	
		} else if(column_id >= 0 && column_id <= 2) {
			for(rowid_t id = row_id; id < row_id + row_count; id++) {
				fact_table_->Set<STRING>(column_id, id, GenerateString(key_width[column_id], data));
			}
		} else {
			exit(-1);
		}
	}

	const View& FactView() {
		return fact_table_->view();
	}
	const View& Dim1View() {
		return dim1_table_->view();
	}

	const View& Dim2View() {
		return dim2_table_->view();
	}

	const View& Dim3View() {
		return dim3_table_->view();
	}
	const TupleSchema& FactSchema() {
		return fact_table_->schema();
	}
	const TupleSchema& Dim1Schema() {
		return dim1_table_->schema();
	}
	const TupleSchema& Dim2Schema() {
		return dim2_table_->schema();
	}
	const TupleSchema& Dim3Schema() {
		return dim3_table_->schema();
	}
private:
	scoped_ptr<Table> fact_table_;
	scoped_ptr<Table> dim1_table_;
	scoped_ptr<Table> dim2_table_;
	scoped_ptr<Table> dim3_table_;
	vector<int> key_width;
	rowcount_t row_group_count_;
};

class MapView {
public:
	MapView(const View& view, 
		const vector<vector<bool>> storage_map) { 
		TupleSchema fact_schema;
		fact_schema.add_attribute(Attribute("key_1",STRING,NOT_NULLABLE));
		fact_schema.add_attribute(Attribute("key_2",STRING,NOT_NULLABLE));
		fact_schema.add_attribute(Attribute("key_3",STRING,NOT_NULLABLE));
		fact_schema.add_attribute(Attribute("agg",INT32,NOT_NULLABLE));
		table_.reset(new Table(fact_schema, HeapBufferAllocator::Get()));
	
		scoped_ptr<Operation> sco(ScanView(view));
		scoped_ptr<Cursor> scan_cursor(SucceedOrDie(sco->CreateCursor()));
		
		vector<vector<StorageType>> storage_type_vector(storage_map.size(),vector<StorageType>());

		vector<rowcount_t> in_memory_row_capacity_vector(view.column_count(), 0);
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
			//in_memory_row_capacity_vector[view.column_count() - 1] += rowGroupSize;
		}

		table_->ReserveRowCapacityOneTime(view.row_count(), in_memory_row_capacity_vector);
		int group_index = 0;
		while(1) {
			ResultView result(scan_cursor->Next(rowGroupSize));
			if(result.has_data()) {
				table_->AppendView(result.view(), storage_type_vector[group_index]);
				group_index++;
			}else{
				break;
			}
		}
	}

	const View& MapedView() {
		return table_->view();
	}
private:
	scoped_ptr<Table> table_;
};

}

#endif
