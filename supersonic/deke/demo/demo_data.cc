#include <iostream>
#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/test_data_generater.h"
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include <vector>
#include <sstream>
#include "deke/include/visit_times_to_locality.h"
#include "deke/include/demo_data.h"
#include "deke/include/cal_io.h"

using std::cout;
using std::endl;
using std::vector;
using std::stringstream;
using namespace supersonic;

int get_time(struct timespec& begin, struct timespec& end) {
	return 1000 * (end.tv_sec - begin.tv_sec) + (end.tv_nsec - begin.tv_nsec) / 1000000;
}

int main(int argc, char* argv[]) {
	
	if(argc !=4) {
		std::cout << "./demo_data row_group_count run_time width" << std::endl;
		exit(-1);
	}
	int rgc = atoi(argv[1]);
	int run_time = atoi(argv[2]);
	int wd = atoi(argv[3]);

	const rowcount_t row_group_size = Optimizer::kRowGroupSize;
	int row_group_count = rgc;
	int query_count = 1;
	int fact_column_count = 4;
	int key_column_count = 3;
	vector<int> column_width = {32, 56, 24, 4};
	vector<double> query_proportion = {1};
	vector<vector<double>> selectivity;
	vector<double> element1 = {0.2, 0.01, 0.4};
	selectivity.push_back(element1);
	
	int memory_limit = rowGroupSize * wd * row_group_count;

	vector<int> key_column_width({32,56,24});
	vector<vector<double>> init_visit_locality(3,vector<double>(row_group_count, 1.0 / row_group_count));
	vector<vector<int>> classic_filter_order(row_group_count,{0,2,1});
	struct timespec begin, end;

	vector<int> group_id;
	group_id.push_back(0);
	group_id.push_back(1);
	group_id.push_back(2);
		
	ViewPrinter vp;

	clock_gettime(CLOCK_REALTIME, &begin);
	DemoDataSet demo_data_set(row_group_count);
	clock_gettime(CLOCK_REALTIME, &end);
	std::cout << "generate data time: " << get_time(begin, end) << " ms." << std::endl;

	Optimizer* optimizer = new Optimizer(row_group_count,
					key_column_count,
					query_count,
					memory_limit,
					&key_column_width,
					&query_proportion,
					&init_visit_locality,
					&selectivity);
	
	

	vector<vector<rowcount_t>> classic_visit_freq;
// init
	clock_gettime(CLOCK_REALTIME, &begin);
	for(int i = 0; i < run_time; i++) {
		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("key_1"));
		ag_gr_p->add(ProjectNamedAttribute("key_2"));
		ag_gr_p->add(ProjectNamedAttribute("key_3"));

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM,"agg","sum_agg");
		
		vector<Operation*> lhs_children;
		lhs_children.push_back(ScanView(demo_data_set.Dim1View()));
		lhs_children.push_back(ScanView(demo_data_set.Dim2View()));
		lhs_children.push_back(ScanView(demo_data_set.Dim3View()));

		//Operation* fact_operation = ScanView(demo_data_set.FactView());
		Operation* fact_operation = ScanView(demo_data_set.FactView());

		scoped_ptr<Operation> ag(MeasureAggregate(
			ag_gr_p,group_id,
			ag_spe,NULL,
			&classic_filter_order,
			lhs_children, 
			fact_operation));	
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));
		
		ResultView resv(cr->Next(-1));
		if(i == run_time -1) {
			classic_visit_freq = demo_data_set.FactView().ColumnPieceVisitTimes(ag_gr_p);
		}
	}
	clock_gettime(CLOCK_REALTIME, &end);
	double init_time = get_time(begin, end) * 1.0 / run_time;
	std::cout << "init time: " << get_time(begin, end) * 1.0 / run_time << " ms." << std::endl;
	
	vector<vector<bool>> classic_storage_map = optimizer->CacheHottestGroup(classic_visit_freq);
	
	clock_gettime(CLOCK_REALTIME, &begin);
	vector<vector<bool>> optimized_storage_map = *optimizer->optimized_storage_map();
	clock_gettime(CLOCK_REALTIME, &end);
	std::cout << "optimized storage map time: " << get_time(begin, end) << " ms." << std::endl;

/*
	for(int i = 0 ; i < optimized_storage_map.size(); i++) {
		for(int j = 0; j < optimized_storage_map[i].size(); j++) {
			cout<<optimized_storage_map[i][j] << "|" << classic_storage_map[i][j]<< "\t";
		}
		cout << endl;
	}
*/	
	vector<vector<int>> filter_order = *optimizer->optimized_filter_order(0);
	/*
	cout<<"optimized filter order " << endl;
	for(int i = 0; i < filter_order.size(); i++) {
		for(int j = 0; j < filter_order[i].size(); j++) {
			cout<< filter_order[i][j] << "\t" <<endl;
		}
		cout << endl;
	}
	cout << "optimized filter order end" << endl;
	*/

	vector<vector<rowcount_t>> visit_freq;
// classic
	MapView map_classic_view(demo_data_set.FactView(), classic_storage_map);
	//map_classic_view.MapedView().PrintViewColumnPieceInfo();
	clock_gettime(CLOCK_REALTIME, &begin);
	for(int i = 0; i < run_time; i++) {
		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("key_1"));
		ag_gr_p->add(ProjectNamedAttribute("key_2"));
		ag_gr_p->add(ProjectNamedAttribute("key_3"));

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM,"agg","sum_agg");
		
		vector<Operation*> lhs_children;
		lhs_children.push_back(ScanView(demo_data_set.Dim1View()));
		lhs_children.push_back(ScanView(demo_data_set.Dim2View()));
		lhs_children.push_back(ScanView(demo_data_set.Dim3View()));

		Operation* fact_operation = ScanView(map_classic_view.MapedView());

		scoped_ptr<Operation> ag(MeasureAggregate(
			ag_gr_p,group_id,
			ag_spe,NULL,
			&classic_filter_order,
			lhs_children, 
			fact_operation));	
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));
		ResultView resv(cr->Next(-1));

		if(i == run_time -1) {
			visit_freq = map_classic_view.MapedView().ColumnPieceVisitTimes(ag_gr_p);
		}
	}
	clock_gettime(CLOCK_REALTIME, &end);
	double classic_time = get_time(begin, end) * 1.0 / run_time;
	int classic_io_times = calculate_io(visit_freq, classic_storage_map, key_column_width);
	std::cout << "classic time: " << get_time(begin, end) * 1.0 / run_time << " ms." << " classic_io_times: " << classic_io_times << std::endl;

	vector<vector<rowcount_t>> optimized_visit_freq;
// optimized
	MapView map_optimized_view(demo_data_set.FactView(), optimized_storage_map);
	//map_optimized_view.MapedView().PrintViewColumnPieceInfo();
	clock_gettime(CLOCK_REALTIME, &begin);
	for(int i = 0; i < run_time; i++) {
		CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
		ag_gr_p->add(ProjectNamedAttribute("key_1"));
		ag_gr_p->add(ProjectNamedAttribute("key_2"));
		ag_gr_p->add(ProjectNamedAttribute("key_3"));

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM,"agg","sum_agg");
		
		vector<Operation*> lhs_children;
		lhs_children.push_back(ScanView(demo_data_set.Dim1View()));
		lhs_children.push_back(ScanView(demo_data_set.Dim2View()));
		lhs_children.push_back(ScanView(demo_data_set.Dim3View()));

		Operation* fact_operation = ScanView(map_optimized_view.MapedView());

		scoped_ptr<Operation> ag(MeasureAggregate(
			ag_gr_p,group_id,
			ag_spe,NULL,
			&filter_order,
			lhs_children, 
			fact_operation));	
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));
		ResultView resv(cr->Next(-1));

		if(i == run_time -1) {
			optimized_visit_freq = map_optimized_view.MapedView().ColumnPieceVisitTimes(ag_gr_p);
		}
	}
	clock_gettime(CLOCK_REALTIME, &end);
	double optimized_time = get_time(begin, end) * 1.0 / run_time;
	int optimized_io_times = calculate_io(optimized_visit_freq, optimized_storage_map, key_column_width);
	std::cout << "optimized time: " << get_time(begin, end) * 1.0 / run_time << " ms." << " optimized_io_times: " << optimized_io_times << std::endl;

	cout << "Result row_group_count = " <<row_group_count
	<< " run_time = " << run_time
	<< " memory width = "<< wd 
	<< " | " << init_time << " " << classic_time << " " << optimized_time << endl; 
	
	return 0;
}

