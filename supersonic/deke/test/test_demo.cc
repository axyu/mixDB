#include <iostream>
#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/test_data_generater.h"
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include <vector>
#include <sstream>
#include "deke/include/visit_times_to_locality.h"

using std::cout;
using std::endl;
using std::vector;
using std::stringstream;
using namespace supersonic;

int get_time(struct timespec& begin, struct timespec& end) {
	return 1000 * (end.tv_sec - begin.tv_sec) + (end.tv_nsec - begin.tv_nsec) / 1000000;
}

int main() {
	const rowcount_t row_group_size = Optimizer::kRowGroupSize;
	int row_group_count = 1000;
	int query_count = 1;
	int fact_column_count = 4;
	int key_column_count = 3;
	vector<int> column_width = {24, 56, 32, 4};
	vector<double> query_proportion = {1};
	/*vector<vector<double>> visit_locality(3, {
		0, 0.05, 0, 0, 0,
		0, 0.05, 0, 0, 0,
		0, 0.05, 0, 0, 0,
		0, 0.05, 0, 0, 0,
		0, 0.05, 0, 0, 0,
		0, 0, 0.05, 0, 0,
		0, 0, 0.05, 0, 0,
		0, 0, 0.05, 0, 0,
		0, 0, 0.05, 0, 0,
		0, 0, 0.05, 0, 0,
		0, 0, 0, 0.05, 0,
		0, 0, 0, 0.05, 0,
		0, 0, 0, 0.05, 0,
		0, 0, 0, 0.05, 0,
		0, 0, 0, 0.05, 0,
		0, 0, 0, 0, 0,
		0.05, 0, 0, 0, 0.05,
		0, 0, 0, 0, 0.05,
		0, 0, 0, 0, 0.05,
		0, 0, 0, 0, 0.05});
*/
	vector<vector<double>> selectivity;
	//vector<double> element1 = {0.6, 0.6, 0.1, 0.6, 0.1};
	//vector<double> element2 = {0.6, 0.5, 0.2, 0.5, 0.02};
	//vector<double> element3 = {0.6, 0.2, 0.2, 0.6, 0.01};
	vector<double> element1 = {0.4, 0.01, 0.2};
	selectivity.push_back(element1);

	TupleSchema fact_schema;
	fact_schema.add_attribute(Attribute("key_1",STRING,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("key_2",STRING,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("key_3",STRING,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("agg",INT32,NOT_NULLABLE));

	vector<TupleSchema> dimension_schemas;
	TupleSchema dimension1_schema, dimension2_schema, dimension3_schema;
	dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));
	dimension2_schema.add_attribute(Attribute("dimension_2",INT32,NULLABLE));
	dimension3_schema.add_attribute(Attribute("dimension_3",DOUBLE,NULLABLE));
	dimension_schemas.push_back(dimension1_schema);
	dimension_schemas.push_back(dimension2_schema);
	dimension_schemas.push_back(dimension3_schema);

	CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
	ag_gr_p->add(ProjectNamedAttribute("key_1"));
	ag_gr_p->add(ProjectNamedAttribute("key_2"));
	ag_gr_p->add(ProjectNamedAttribute("key_3"));

	vector<vector<double>> init_visit_locality(3, vector<double>(1000, 0.001));
	TestDataGenerater tg(fact_schema,
			dimension_schemas,
			row_group_size,
			row_group_count,
			query_count,
			fact_column_count,
			key_column_count,
			ag_gr_p,
			&column_width,
			&init_visit_locality,
			&selectivity);
	const TestDataSet* test_data_set = tg.GenerateDataSet();

	int memory_limit = 1024 * 56 * 100;
	//vector<double> init_visit_locality = {0.2, 0.2, 0.2, 0.2, 0.2};
	//vector<double> init_visit_locality = {0.05, 0.02, 0.1, 0.8, 0.03};

	vector<int> key_column_width = {24, 56, 32};
	Optimizer* optimizer = new Optimizer(row_group_count,
			key_column_count,
			query_count,
			memory_limit,
			&key_column_width,
			&query_proportion,
			&init_visit_locality,
			&selectivity);
/*
	vector<vector<double>> init_visit_locality(5, {
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0, 0, 0, 0, 0,
		0.05, 0.05, 0.05, 0.05, 0.05,
		0.05, 0.05, 0.05, 0.05, 0.05,
		0.05, 0.05, 0.05, 0.05, 0.05,
		0.05, 0.05, 0.05, 0.05, 0.05});
*/

	//std::cout << "optimizer over" << std::endl;

	vector<vector<const View*>> dimension_views_vector;
	for(int query_id = 0; query_id < query_count; query_id++) {
		vector<const View*> dimension_views;
		for(int i = 0; i < key_column_count; i++) {
			dimension_views.push_back(&test_data_set->DimensionViews(query_id, i));
		}
		dimension_views_vector.push_back(dimension_views);
	}

	//std::cout<<"read fact table end"<<std::endl;
	std::cout<<">>>>>>>>>>>>calculate start<<<<<<<<<<<<<<"<<std::endl;
	

	vector<vector<bool>> init_storage_map(row_group_count, vector<bool>(key_column_count, false));
	struct timespec begin, end;
	
	vector<vector<vector<int>>> classic_filter_order;
	vector<vector<int>> classic_filter_order_q1(1000, {0, 2, 1});
	classic_filter_order.push_back(classic_filter_order_q1);
	const View& classic_init_fact_view = test_data_set->FactView(init_storage_map);
	vector<double> query_choice = {1};

// classic initial 
	clock_gettime(CLOCK_REALTIME, &begin);
	for(int i = 0; i < 10; i++) {
		vector<vector<Operation*>> lhs_children_vector;
		for(int query_id_it = 0; query_id_it < query_count; query_id_it++) {
			vector<Operation*> lhs_children;
			for(int key_id = 0; key_id < dimension_schemas.size(); key_id++) {
				lhs_children.push_back(ScanView(*dimension_views_vector[query_id_it][key_id]));
				//std::cout<<"read dimension table end "<<i<<std::endl;
			}
			lhs_children_vector.push_back(lhs_children);
		}
		Operation* fact_operation = ScanView(classic_init_fact_view);
		double poss = (double)i / 10;
		int query_id = 0;
		for(int j = 0; j < query_count; j++) {
			if(poss < query_choice[j]) {
				query_id = j;
				break;
			}
		}
		//std::cout << "query_id: " << query_id << std::endl;
		CompoundSingleSourceProjector *ag_gr_p_temp = new CompoundSingleSourceProjector();
		ag_gr_p_temp->add(ProjectNamedAttribute("key_1"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_2"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_3"));

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM,"agg","sum_agg");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p_temp,ag_spe,NULL,&classic_filter_order[query_id],lhs_children_vector[query_id], fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		//std::cout<<"begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		//ViewPrinter view_printer;
		//view_printer.AppendViewToStream(rv, &cout);
	}

	clock_gettime(CLOCK_REALTIME, &end);
	std::cout << "classic initial time: "	<< get_time(begin, end) << " ms." << std::endl << std::endl;

// classic query
	vector<vector<rowcount_t>> classic_visit_freq = classic_init_fact_view.ColumnPieceVisitTimes(ag_gr_p);
	vector<vector<bool>> classic_storage_map = optimizer->CacheHottestGroup(classic_visit_freq);
	//vector<vector<bool>> classic_storage_map(row_group_count, {true, false, true});
	//vector<vector<vector<int>>> classic_filter_order;
	//vector<vector<int>> classic_filter_order_q1(100, {2, 0, 1});
	//classic_filter_order.push_back(classic_filter_order_q1);
	for(int i = 0; i < classic_storage_map.size(); i++) {
		for(int j = 0; j < classic_storage_map[i].size(); j++) {
			std::cout <<"storage: " <<classic_storage_map[i][j]<< " ";
		}
		std::cout << std::endl;
	}
	const View& classic_query_fact_view = test_data_set->FactView(classic_storage_map);

	clock_gettime(CLOCK_REALTIME, &begin);
	for(int i = 0; i < 10; i++) {
		vector<vector<Operation*>> lhs_children_vector;
		for(int query_id_it = 0; query_id_it < query_count; query_id_it++) {
			vector<Operation*> lhs_children;
			for(int key_id = 0; key_id < dimension_schemas.size(); key_id++) {
				lhs_children.push_back(ScanView(*dimension_views_vector[query_id_it][key_id]));
				//std::cout<<"read dimension table end "<<i<<std::endl;
			}
			lhs_children_vector.push_back(lhs_children);
		}
		Operation* fact_operation = ScanView(classic_query_fact_view);
		double poss = (double)i / 10;
		int query_id = 0;
		for(int j = 0; j < query_count; j++) {
			if(poss < query_choice[j]) {
				query_id = j;
				break;
			}
		}
		//std::cout << "query_id: " << query_id << std::endl;
		CompoundSingleSourceProjector *ag_gr_p_temp = new CompoundSingleSourceProjector();
		ag_gr_p_temp->add(ProjectNamedAttribute("key_1"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_2"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_3"));

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM,"agg","sum_agg");
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p_temp,ag_spe,NULL,&classic_filter_order[query_id],lhs_children_vector[query_id], fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		//std::cout<<"begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		//ViewPrinter view_printer;
		//view_printer.AppendViewToStream(rv, &cout);
	}

	clock_gettime(CLOCK_REALTIME, &end);
	std::cout << "classic query time: "	<< get_time(begin, end) << " ms." << std::endl << std::endl;
	//vector<vector<rowcount_t>> optimized_visit_freq = classic_query_fact_view.ColumnPieceVisitTimes(ag_gr_p);


// optimized initial 
/*
	const vector<vector<bool>>* optimized_init_storage_map = optimizer->optimized_storage_map();
	for(int i = 0; i < optimized_init_storage_map->size(); i++) {
		for(int j = 0; j < optimized_init_storage_map->at(i).size(); j++) {
			std::cout <<"storage: " << optimized_init_storage_map->at(i)[j]<< " ";
		}
		std::cout << std::endl;
	}
	const View& optimized_init_fact_view = test_data_set->FactView(*optimized_init_storage_map);
	clock_gettime(CLOCK_REALTIME, &begin);

	for(int i = 0; i < 10; i++) {
		vector<vector<Operation*>> lhs_children_vector;
		for(int query_id_it = 0; query_id_it < query_count; query_id_it++) {
			vector<Operation*> lhs_children;
			for(int key_id = 0; key_id < dimension_schemas.size(); key_id++) {
				lhs_children.push_back(ScanView(*dimension_views_vector[query_id_it][key_id]));
				//std::cout<<"read dimension table end "<<i<<std::endl;
			}
			lhs_children_vector.push_back(lhs_children);
		}
		Operation* fact_operation = ScanView(optimized_init_fact_view);
		double poss = (double)i / 10;
		int query_id = 0;
		for(int j = 0; j < query_count; j++) {
			if(poss < query_choice[j]) {
				query_id = j;
				break;
			}
		}
		//std::cout << "query_id: " << query_id << std::endl;
		CompoundSingleSourceProjector *ag_gr_p_temp = new CompoundSingleSourceProjector();
		ag_gr_p_temp->add(ProjectNamedAttribute("key_1"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_2"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_3"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_4"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_5"));

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM,"agg","sum_agg");
		const vector<vector<int>>* filter_order = optimizer->optimized_filter_order(query_id);
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p_temp,ag_spe,NULL,filter_order,lhs_children_vector[query_id], fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		//std::cout<<"begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		ViewPrinter view_printer;
		view_printer.AppendViewToStream(rv, &cout);
	}

	clock_gettime(CLOCK_REALTIME, &end);

	std::cout << "optimized initial time: "	<< get_time(begin, end) << " ms." << std::endl << std::endl;
*/

//optimized query
	//vector<vector<rowcount_t>> optimized_visit_freq = optimized_init_fact_view.ColumnPieceVisitTimes(ag_gr_p);

	//vector<vector<double>> lo = visit_times_to_locality(optimized_visit_freq);

	optimizer->ReConfigure(row_group_count,
			key_column_count,
			query_count,
			memory_limit,
			&key_column_width,
			&query_proportion,
			&init_visit_locality,
			&selectivity);
	const vector<vector<bool>>* optimized_storage_map = optimizer->optimized_storage_map();
	for(int i = 0; i < optimized_storage_map->size(); i++) {
		for(int j = 0; j < optimized_storage_map->at(i).size(); j++) {
			std::cout <<"storage: " << optimized_storage_map->at(i)[j]<< " ";
		}
		std::cout << std::endl;
	}
	const View& optimized_query_fact_view = test_data_set->FactView(*optimized_storage_map);

	clock_gettime(CLOCK_REALTIME, &begin);
	for(int i = 0; i < 10; i++) {
		vector<vector<Operation*>> lhs_children_vector;
		for(int query_id_it = 0; query_id_it < query_count; query_id_it++) {
			vector<Operation*> lhs_children;
			for(int key_id = 0; key_id < dimension_schemas.size(); key_id++) {
				lhs_children.push_back(ScanView(*dimension_views_vector[query_id_it][key_id]));
				//std::cout<<"read dimension table end "<<i<<std::endl;
			}
			lhs_children_vector.push_back(lhs_children);
		}
		Operation* fact_operation = ScanView(optimized_query_fact_view);
		double poss = (double)i / 10;
		int query_id = 0;
		for(int j = 0; j < query_count; j++) {
			if(poss < query_choice[j]) {
				query_id = j;
				break;
			}
		}
		//std::cout << "query_id: " << query_id << std::endl;
		CompoundSingleSourceProjector *ag_gr_p_temp = new CompoundSingleSourceProjector();
		ag_gr_p_temp->add(ProjectNamedAttribute("key_1"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_2"));
		ag_gr_p_temp->add(ProjectNamedAttribute("key_3"));

		AggregationSpecification *ag_spe = new AggregationSpecification();
		ag_spe->AddAggregation(SUM,"agg","sum_agg");
		const vector<vector<int>>* filter_order = optimizer->optimized_filter_order(query_id);
		scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p_temp,ag_spe,NULL,filter_order,lhs_children_vector[query_id], fact_operation));
		scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

		//std::cout<<"begin caculate"<<std::endl;

		ResultView resv(ResultView(cr->Next(-1)));
		View rv(resv.view());
		//ViewPrinter view_printer;
		//view_printer.AppendViewToStream(rv, &cout);
	}

	clock_gettime(CLOCK_REALTIME, &end);
	std::cout << "optimized query time: "	<< get_time(begin, end) << " ms." << std::endl << std::endl;

	vector<vector<rowcount_t>> optimized_visit_freq = optimized_query_fact_view.ColumnPieceVisitTimes(ag_gr_p);

	return 0;
}
