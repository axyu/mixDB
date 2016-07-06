#include <iostream>
#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/cursor/infrastructure/view_printer.h"
#include "supersonic/base/infrastructure/test_data_generater.h"
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include <vector>
#include <sstream>
#include "deke/include/visit_times_to_locality.h"
#include "deke/include/ssb_q3_2.h"

using std::cout;
using std::endl;
using std::vector;
using std::stringstream;
using namespace supersonic;


ViewPrinter view_printer;

int get_time(struct timespec& begin, struct timespec& end) {
	return 1000 * (end.tv_sec - begin.tv_sec) + (end.tv_nsec - begin.tv_nsec) / 1000000;
}

const View& LineorderView(vector<vector<bool>>& storage_map) {
	int lineorder_row_count = 6001215;
	TupleSchema lineorder_schema = SSB_Q3_2_Schema::lineorder();	
	File* fp = File::OpenOrDie("../../storage/lineorder_q3_2.tbl", "r");
	FailureOrOwned<Cursor> cu(FileInput(lineorder_schema, fp, false, HeapBufferAllocator::Get()));
	Table* table = new Table(lineorder_schema, HeapBufferAllocator::Get());
	vector<vector<StorageType>> storage_type_vector(storage_map.size(), vector<StorageType>(lineorder_schema.attribute_count(), DISK));
	vector<rowcount_t> in_memory_row_capacity_vector(lineorder_schema.attribute_count(), 0);
	for(int map_row_id = 0; map_row_id < storage_map.size(); map_row_id++) {
		if(storage_map[map_row_id][0]) {
			in_memory_row_capacity_vector[2] += rowGroupSize;
			storage_type_vector[map_row_id][2] = MEMORY;
		}
		if(storage_map[map_row_id][1]) {
			in_memory_row_capacity_vector[4] += rowGroupSize;
			storage_type_vector[map_row_id][4] = MEMORY;
		}
		if(storage_map[map_row_id][2]) {
			in_memory_row_capacity_vector[5] += rowGroupSize;
			storage_type_vector[map_row_id][5] = MEMORY;
		}
	}
	table->ReserveRowCapacityOneTime(lineorder_row_count, in_memory_row_capacity_vector);
	int storage_type_row_index = 0;
	while(1) {
		ResultView result(cu->Next(rowGroupSize));
		if(result.has_data())
		{
			std::cout << "storage_type_row_index: " << storage_type_row_index<< std::endl;
			//view_printer.AppendViewToStream(result.view(), &std::cout);
			table->AppendView(result.view(), storage_type_vector[storage_type_row_index]);
			storage_type_row_index++;
		} else {
			break;
		}
	}
	return table->view();
}

const View* CustomerView() {
	int customer_row_count = 30001;
	TupleSchema customer_schema = SSB_Q3_2_Schema::customer();
	File* fp = File::OpenOrDie("../../storage/customer_q3_2.tbl", "r");
	FailureOrOwned<Cursor> cu(FileInput(customer_schema, fp, false, HeapBufferAllocator::Get()));
	Table* table = new Table(customer_schema, HeapBufferAllocator::Get());
	vector<StorageType> storage_type(1, MEMORY);
	vector<rowcount_t> in_memory_row_capacity_vector(1, customer_row_count);
	table->ReserveRowCapacityOneTime(customer_row_count, in_memory_row_capacity_vector);
	while(1) {
		ResultView result(cu->Next(rowGroupSize));
		if(result.has_data()) {
			table->AppendView(result.view(), storage_type);
		} else {
			break;
		}
	}
	return &table->view();
}

const View* SupplierView() {
	int supplier_row_count = 2001;
	TupleSchema supplier_schema = SSB_Q3_2_Schema::supplier();
	File* fp = File::OpenOrDie("../../storage/supplier_q3_2.tbl", "r");
	FailureOrOwned<Cursor> cu(FileInput(supplier_schema, fp, false, HeapBufferAllocator::Get()));
	Table* table = new Table(supplier_schema, HeapBufferAllocator::Get());
	vector<StorageType> storage_type(1, MEMORY);
	vector<rowcount_t> in_memory_row_capacity_vector(1, supplier_row_count);
	table->ReserveRowCapacityOneTime(supplier_row_count, in_memory_row_capacity_vector);
	while(1) {
		ResultView result(cu->Next(rowGroupSize));
		if(result.has_data()) {
			table->AppendView(result.view(), storage_type);
		} else {
			break;
		}
	}
	return &table->view();
}

const View* DateView() {
	int date_row_count = 2556;
	TupleSchema date_schema = SSB_Q3_2_Schema::date();
	File* fp = File::OpenOrDie("../../storage/date_q3_2.tbl", "r");
	FailureOrOwned<Cursor> cu(FileInput(date_schema, fp, false, HeapBufferAllocator::Get()));
	Table* table = new Table(date_schema, HeapBufferAllocator::Get());
	vector<StorageType> storage_type(1, MEMORY);
	vector<rowcount_t> in_memory_row_capacity_vector(1, date_row_count);
	table->ReserveRowCapacityOneTime(date_row_count, in_memory_row_capacity_vector);
	while(1) {
		ResultView result(cu->Next(rowGroupSize));
		if(result.has_data()) {
			table->AppendView(result.view(), storage_type);
		} else {
			break;
		}
	}
	return &table->view();
}

int main() {
	const rowcount_t row_group_size = Optimizer::kRowGroupSize;
	int row_group_count = 1000;
	int query_count = 1;
	int fact_column_count = 17;
	int key_column_count = 3;
	vector<int> column_width = {8, 8, 8, 8, 8, 8, 128, 128, 8, 8, 8, 8, 8, 8, 8, 8, 128};
	vector<double> query_proportion = {1};
	vector<vector<double>> selectivity;
	vector<double> element = {1.0 / 25.0, 1.0 / 25.0, 6.0 / 7.0};
	selectivity.push_back(element);

	TupleSchema fact_schema = SSB_Q3_2_Schema::lineorder();
	vector<TupleSchema> dimension_schemas;
	TupleSchema dimension1_schema = SSB_Q3_2_Schema::customer();
	TupleSchema dimension2_schema = SSB_Q3_2_Schema::supplier();
	TupleSchema dimension3_schema = SSB_Q3_2_Schema::date();
	dimension_schemas.push_back(dimension1_schema);
	dimension_schemas.push_back(dimension2_schema);
	dimension_schemas.push_back(dimension3_schema);

	vector<vector<double>> init_visit_locality(3, vector<double>(1000, 0.001));

	int memory_limit = rowGroupSize * 24 * row_group_count;

	vector<int> key_column_width = {8, 8, 8};

	/*
	   Optimizer* optimizer = new Optimizer(row_group_count,
	   key_column_count,
	   query_count,
	   memory_limit,
	   &key_column_width,
	   &query_proportion,
	   &init_visit_locality,
	   &selectivity);
	   */
	vector<const View*> dimension_views;
	dimension_views.push_back(CustomerView());
	dimension_views.push_back(SupplierView());
	dimension_views.push_back(DateView());

	std::cout<<">>>>>>>>>>>>calculate start<<<<<<<<<<<<<<"<<std::endl;

	vector<vector<bool>> init_storage_map(row_group_count, vector<bool>(key_column_count, false));
	struct timespec begin, end;

	vector<vector<int>> classic_filter_order(1000, {0, 2, 1});
	const View& classic_init_fact_view = LineorderView(init_storage_map);

	// classic initial 
	clock_gettime(CLOCK_REALTIME, &begin);
	vector<Operation*> lhs_children;
	for(int key_id = 0; key_id < dimension_schemas.size(); key_id++) {
		lhs_children.push_back(ScanView(*dimension_views[key_id]));
	}
	ViewPrinter view_printer;
	//view_printer.AppendViewToStream(classic_init_fact_view, &cout);
	Operation* fact_operation = ScanView(classic_init_fact_view);

	CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
	ag_gr_p->add(ProjectNamedAttribute("LO_CUSTKEY"));
	ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
	ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

	vector<int> group_id;
	group_id.push_back(0);
	group_id.push_back(1);
	group_id.push_back(2);

	AggregationSpecification *ag_spe = new AggregationSpecification();
	ag_spe->AddAggregation(SUM, "LO_REVENUE", "revenue");
	scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &classic_filter_order, lhs_children, fact_operation));
	scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

	std::cout<<"begin caculate"<<std::endl;

	ResultView resv(ResultView(cr->Next(-1)));
	View rv(resv.view());
	std::cout << "result" << std::endl;
	//ViewPrinter view_printer;
	view_printer.AppendViewToStream(rv, &cout);

	clock_gettime(CLOCK_REALTIME, &end);
	std::cout << "classic initial time: "	<< get_time(begin, end) << " ms." << std::endl << std::endl;

	return 0;
}

