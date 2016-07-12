// final test 1
// ssb query: 1/3 2.1 + 1/3 2.2 + 1/3 2.3

#include <iostream>
#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/block.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/test_data_generater.h"
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include <vector>
#include <sstream>
#include "deke/include/visit_times_to_locality.h"
#include "deke/include/cal_io.h"
#include "deke/include/ssb_q2_1.h"
#include "deke/include/ssb_q2_2.h"
#include "deke/include/ssb_q2_3.h"

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
	int lineorder_row_count = 300005811;
	TupleSchema lineorder_schema = SSB_Q2_1_Schema::lineorder();	
	File* fp = File::OpenOrDie("../../storage/lineorder_q2_1.tbl", "r");
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
			in_memory_row_capacity_vector[3] += rowGroupSize;
			storage_type_vector[map_row_id][3] = MEMORY;
		}
		if(storage_map[map_row_id][2]) {
			in_memory_row_capacity_vector[4] += rowGroupSize;
			storage_type_vector[map_row_id][4] = MEMORY;
		}
		if(storage_map[map_row_id][3]) {
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
			//view_printer.AppendViewToStream(result.view(), &std::cout);
			table->AppendView(result.view(), storage_type_vector[storage_type_row_index]);
			storage_type_row_index++;
		} else {
			break;
		}
	}
	return table->view();
}

const View* SupplierView_Q2_1() {
	int supplier_row_count = 100001;
	TupleSchema supplier_schema = SSB_Q2_1_Schema::supplier();
	File* fp = File::OpenOrDie("../../storage/supplier_q2_1.tbl", "r");
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


const View* PartView_Q2_1() {
	int supplier_row_count = 1200001;
	TupleSchema supplier_schema = SSB_Q2_1_Schema::part();
	File* fp = File::OpenOrDie("../../storage/part_q2_1.tbl", "r");
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

const View* DateView_Q2_1() {
	int date_row_count = 2556;
	TupleSchema date_schema = SSB_Q2_1_Schema::date();
	File* fp = File::OpenOrDie("../../storage/date_q2_1.tbl", "r");
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

// dimension tables for Q2_2 with different selectivity
const View* SupplierView_Q2_2() {
	int supplier_row_count = 100001;
	TupleSchema supplier_schema = SSB_Q2_2_Schema::supplier();
	File* fp = File::OpenOrDie("../../storage/supplier_q2_2.tbl", "r");
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


const View* PartView_Q2_2() {
	int supplier_row_count = 1200001;
	TupleSchema supplier_schema = SSB_Q2_2_Schema::part();
	File* fp = File::OpenOrDie("../../storage/part_q2_2.tbl", "r");
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

const View* DateView_Q2_2() {
	int date_row_count = 2556;
	TupleSchema date_schema = SSB_Q2_2_Schema::date();
	File* fp = File::OpenOrDie("../../storage/date_q2_2.tbl", "r");
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

const View* SupplierView_Q2_3() {
	int supplier_row_count = 100001;
	TupleSchema supplier_schema = SSB_Q2_3_Schema::supplier();
	File* fp = File::OpenOrDie("../../storage/supplier_q2_3.tbl", "r");
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


const View* PartView_Q2_3() {
	int supplier_row_count = 1200001;
	TupleSchema supplier_schema = SSB_Q2_3_Schema::part();
	File* fp = File::OpenOrDie("../../storage/part_q2_3.tbl", "r");
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

const View* DateView_Q2_3() {
	int date_row_count = 2556;
	TupleSchema date_schema = SSB_Q2_3_Schema::date();
	File* fp = File::OpenOrDie("../../storage/date_q2_3.tbl", "r");
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

int main(int argc, char** argv) {
	if(argc != 5) {
		std::cout << "./this q2_1_run_time q2_2_run_time q2_3_run_time memory_width" << std::endl;
		exit(-1);
	}

	int q2_1_run_time = atoi(argv[1]);
	int q2_2_run_time = atoi(argv[2]);
	int q2_3_run_time = atoi(argv[3]);
	int memory_width = atoi(argv[4]);

	const rowcount_t row_group_size = Optimizer::kRowGroupSize;
	int row_group_count = 1000;
	int query_count = 3;
	int fact_column_count = 17;
	int key_column_count = 4;
	vector<int> column_width = {8, 8, 8, 8, 8, 8, 128, 128, 8, 8, 8, 8, 8, 8, 8, 8, 128};
	vector<double> query_proportion;// = {0.5, 0.5};
	query_proportion.push_back(q2_1_run_time * 1.0 / (q2_1_run_time + q2_2_run_time + q2_3_run_time));
	query_proportion.push_back(q2_2_run_time * 1.0 / (q2_1_run_time + q2_2_run_time + q2_3_run_time));
	query_proportion.push_back(q2_3_run_time * 1.0 / (q2_1_run_time + q2_2_run_time + q2_3_run_time));

	std::cout << "query_proportion : " << query_proportion[0] << " " << query_proportion[1] << " " << query_proportion[2] << std::endl;	

	vector<vector<double>> selectivity;
	// cuskey partkey suppkey orderdate
	vector<double> element1 = {-1, 1.0/25, 1.0/5, 1};
	vector<double> element2 = {-1, 1.0/125, 1.0/5, 1};
	vector<double> element3 = {-1, 1.0/1000, 1.0/5, 1};
	selectivity.push_back(element1);
	selectivity.push_back(element2);
	selectivity.push_back(element3);

	TupleSchema fact_schema = SSB_Q2_1_Schema::lineorder();

	vector<TupleSchema> dimension_schemas_Q2_1;
	TupleSchema dimension1_schema_Q2_1 = SSB_Q2_1_Schema::part();
	TupleSchema dimension2_schema_Q2_1 = SSB_Q2_1_Schema::supplier();
	TupleSchema dimension3_schema_Q2_1 = SSB_Q2_1_Schema::date();
	dimension_schemas_Q2_1.push_back(dimension1_schema_Q2_1);
	dimension_schemas_Q2_1.push_back(dimension2_schema_Q2_1);
	dimension_schemas_Q2_1.push_back(dimension3_schema_Q2_1);

	vector<TupleSchema> dimension_schemas_Q2_2;
	TupleSchema dimension1_schema_Q2_2 = SSB_Q2_2_Schema::part();
	TupleSchema dimension2_schema_Q2_2 = SSB_Q2_2_Schema::supplier();
	TupleSchema dimension3_schema_Q2_2 = SSB_Q2_2_Schema::date();
	dimension_schemas_Q2_2.push_back(dimension1_schema_Q2_2);
	dimension_schemas_Q2_2.push_back(dimension2_schema_Q2_2);
	dimension_schemas_Q2_2.push_back(dimension3_schema_Q2_2);

	vector<TupleSchema> dimension_schemas_Q2_3;
	TupleSchema dimension1_schema_Q2_3 = SSB_Q2_3_Schema::part();
	TupleSchema dimension2_schema_Q2_3 = SSB_Q2_3_Schema::supplier();
	TupleSchema dimension3_schema_Q2_3 = SSB_Q2_3_Schema::date();
	dimension_schemas_Q2_3.push_back(dimension1_schema_Q2_3);
	dimension_schemas_Q2_3.push_back(dimension2_schema_Q2_3);
	dimension_schemas_Q2_3.push_back(dimension3_schema_Q2_3);
	

	vector<vector<double>> init_visit_locality(4, vector<double>(1000, 0.001));
/*
	for(int i = 0; i < 750; i++) {
		init_visit_locality[3][i] = 1 * 1.0 / 750;
	}
	for(int i = 750; i < 1000; i++) {
		init_visit_locality[3][i] = 0.0;
	}
*/
	long memory_limit = rowGroupSize * memory_width * row_group_count;

	vector<int> key_column_width = {8, 8, 8, 8};


	Optimizer* optimizer = new Optimizer(row_group_count,
		key_column_count,
		query_count,
		memory_limit,
		&key_column_width,
		&query_proportion,
		&init_visit_locality,
		&selectivity);

	vector<const View*> dimension_views_Q2_1;
	dimension_views_Q2_1.push_back(PartView_Q2_1());
	dimension_views_Q2_1.push_back(SupplierView_Q2_1());
	dimension_views_Q2_1.push_back(DateView_Q2_1());

	vector<const View*> dimension_views_Q2_2;
	dimension_views_Q2_2.push_back(PartView_Q2_2());
	dimension_views_Q2_2.push_back(SupplierView_Q2_2());
	dimension_views_Q2_2.push_back(DateView_Q2_2());

	vector<const View*> dimension_views_Q2_3;
	dimension_views_Q2_3.push_back(PartView_Q2_3());
	dimension_views_Q2_3.push_back(SupplierView_Q2_3());
	dimension_views_Q2_3.push_back(DateView_Q2_3());

	std::cout<<">>>>>>>>>>>>calculate start<<<<<<<<<<<<<<"<<std::endl;

	vector<vector<bool>> init_storage_map(row_group_count, vector<bool>(key_column_count, false));
	struct timespec begin, end;

	vector<vector<int>> classic_filter_order_Q2_1(1000, {0, 1, 2});
	vector<vector<int>> classic_filter_order_Q2_2(1000, {0, 1, 2});
	vector<vector<int>> classic_filter_order_Q2_3(1000, {0, 1, 2});
	
	
/*	
	const View& classic_init_fact_view = LineorderView(init_storage_map);
	// classic initial 
	clock_gettime(CLOCK_REALTIME, &begin);
	// query 2.1
	for(int i = 0; i < q2_1_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_1.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_1[key_id]));
		}
		Operation* fact_operation = ScanView(classic_init_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &classic_filter_order_Q2_1, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"q2_1 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "q2_1 result" << std::endl;
		ViewPrinter view_printer;
		view_printer.AppendViewToStream(rv, &cout);
	}

	// query 2.2
	for(int i = 0; i < q2_2_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_2.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_2[key_id]));
		}
		Operation* fact_operation = ScanView(classic_init_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &classic_filter_order_Q2_2, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"q2_2 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "q2_2 result" << std::endl;
		ViewPrinter view_printer;
		view_printer.AppendViewToStream(rv, &cout);
	}

	// query 2.3
	for(int i = 0; i < q2_3_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_3.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_3[key_id]));
		}
		Operation* fact_operation = ScanView(classic_init_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &classic_filter_order_Q2_3, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"q2_3 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "q2_3 result" << std::endl;
		ViewPrinter view_printer;
		view_printer.AppendViewToStream(rv, &cout);
	}

	clock_gettime(CLOCK_REALTIME, &end);

	std::cout << "classic initial time: "	<< get_time(begin, end) << " ms." << std::endl << std::endl;

	long initial_time = get_time(begin, end);
*/

	CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
	ag_gr_p->add(ProjectNamedAttribute("LO_CUSTKEY"));
	ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
	ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
	ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

/*
	// classic query 
	vector<vector<rowcount_t>> classic_visit_freq = classic_init_fact_view.ColumnPieceVisitTimes(ag_gr_p);
	vector<vector<bool>> classic_storage_map = optimizer->CacheHottestGroup(classic_visit_freq);
	
	const View& classic_fact_view = LineorderView(classic_storage_map);
	clock_gettime(CLOCK_REALTIME, &begin);
	// query 2.1
	for(int i = 0; i < q2_1_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_1.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_1[key_id]));
		}
		Operation* fact_operation = ScanView(classic_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &classic_filter_order_Q2_1, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"classic q2_1 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "classic q2_1 result" << std::endl;
		// ViewPrinter view_printer;
		// view_printer.AppendViewToStream(rv, &cout);
	}
	
	// query 2.2
	for(int i = 0; i < q2_2_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_2.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_2[key_id]));
		}
		Operation* fact_operation = ScanView(classic_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &classic_filter_order_Q2_2, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"classic q2_2 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "classic q2_2 result" << std::endl;
		// ViewPrinter view_printer;
		// view_printer.AppendViewToStream(rv, &cout);
	}

	// query 2.3
	for(int i = 0; i < q2_3_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_3.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_3[key_id]));
		}
		Operation* fact_operation = ScanView(classic_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &classic_filter_order_Q2_3, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"classic q2_3 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "classic q2_3 result" << std::endl;
		// ViewPrinter view_printer;
		// view_printer.AppendViewToStream(rv, &cout);
	}
	clock_gettime(CLOCK_REALTIME, &end);

	vector<vector<rowcount_t>> visit_freq = classic_fact_view.ColumnPieceVisitTimes(ag_gr_p);
	long classic_io_times = calculate_io(visit_freq, classic_storage_map, key_column_width);

	std::cout << "classic query time: "	<< get_time(begin, end) << " ms." << " classic io times: " << classic_io_times << std::endl << std::endl;
	long classic_time = get_time(begin, end);
*/
	
	// optimized query
	clock_gettime(CLOCK_REALTIME, &begin);
	vector<vector<bool>> optimized_storage_map = *optimizer->optimized_storage_map();
	vector<vector<int>> optimized_filter_order_Q2_1 = *optimizer->optimized_filter_order(0);
	vector<vector<int>> optimized_filter_order_Q2_2 = *optimizer->optimized_filter_order(1);
	vector<vector<int>> optimized_filter_order_Q2_3 = *optimizer->optimized_filter_order(2);
	clock_gettime(CLOCK_REALTIME, &end);
	std::cout << "optimized time: " << get_time(begin, end) << " ms. " << std::endl;


/*
	std::cout << ">>>>storage map start<<<<" << std::endl;
	for(int i = 0; i < optimized_storage_map.size(); i++) {
		for(int j = 0 ; j < optimized_storage_map[i].size(); j++) {
			std::cout << optimized_storage_map[i][j] << "|" << classic_storage_map[i][j] << "\t";
		}
		std::cout << std::endl;
	}
	std::cout << ">>>>storage map end<<<<" << std::endl;
*/
		
	long optimize_time = get_time(begin, end);

	const View& optimized_fact_view = LineorderView(optimized_storage_map);
	clock_gettime(CLOCK_REALTIME, &begin);
	// query 2.1
	for(int i = 0; i < q2_1_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_1.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_1[key_id]));
		}
		Operation* fact_operation = ScanView(optimized_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &optimized_filter_order_Q2_1, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"optimized q2_1 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "optimized q2_1 result" << std::endl;
		// ViewPrinter view_printer;
		// view_printer.AppendViewToStream(rv, &cout);
	}
	
	// query 2.2
	for(int i = 0; i < q2_2_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_2.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_2[key_id]));
		}
		Operation* fact_operation = ScanView(optimized_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &optimized_filter_order_Q2_2, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"optimized q2_2 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "optimized q2_2 result" << std::endl;
		// ViewPrinter view_printer;
		// view_printer.AppendViewToStream(rv, &cout);
	}

	// query 2.3
	for(int i = 0; i < q2_3_run_time; i++) {
		vector<Operation*> lhs_children;
		for(int key_id = 0; key_id < dimension_schemas_Q2_3.size(); key_id++) {
			lhs_children.push_back(ScanView(*dimension_views_Q2_3[key_id]));
		}
		Operation* fact_operation = ScanView(optimized_fact_view);

		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("LO_PARTKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_SUPPKEY"));
		ag_gr_p->add(ProjectNamedAttribute("LO_ORDERDATE"));

		vector<int> group_id;
		group_id.push_back(0);
		group_id.push_back(2);

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM, "LO_REVENUE", "SUM_REVENUE");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p, group_id, ag_spe, NULL, &optimized_filter_order_Q2_3, lhs_children, fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		std::cout<<"optimized q2_3 begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		std::cout << "optimized q2_3 result" << std::endl;
		// ViewPrinter view_printer;
		// view_printer.AppendViewToStream(rv, &cout);
	}
	clock_gettime(CLOCK_REALTIME, &end);

	vector<vector<rowcount_t>> optimized_visit_freq = optimized_fact_view.ColumnPieceVisitTimes(ag_gr_p);
	long optimized_io_times = calculate_io(optimized_visit_freq, optimized_storage_map, key_column_width);

	std::cout << "optimized query time: "	<< get_time(begin, end) << " ms." << " optimized io times: " << optimized_io_times << std::endl << std::endl;

	long optimized_time = get_time(begin, end);

	std::cout << "Result"
	<< " q2_1_run_time = " << q2_1_run_time
	<< " q2_2_run_time = " << q2_2_run_time
	<< " q2_3_run_time = " << q2_3_run_time
	<< " memory_width " << memory_width
	<< " optimize_time = " << optimize_time
//	<< " initial_time = " << initial_time
//	<< " classic_time = " << classic_time
	<< " optimized_time = " << optimized_time
	<< std::endl;
	
	return 0;
}

