#include <iostream>
#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/test_data_generater.h"
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include <vector>
#include <sstream>
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
	int row_group_count = 100;
	int query_count = 3;
	int fact_column_count = 6;
	int key_column_count = 5;
	vector<int> column_width = {4, 40, 8, 48, 80, 4};
	vector<double> query_proportion = {0.2, 0.2, 0.6};
	vector<vector<double>> visit_locality(5, {
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

	vector<vector<double>> selectivity;
	//vector<double> element1 = {0.6, 0.6, 0.1, 0.6, 0.1};
	//vector<double> element2 = {0.6, 0.5, 0.2, 0.5, 0.02};
	//vector<double> element3 = {0.6, 0.2, 0.2, 0.6, 0.01};
	vector<double> element1 = {0.01, 0.05, 0.04, 0.03, 0.18};
	vector<double> element2 = {0.03, 0.03, 0.03, 0.06, 0.05};
	vector<double> element3 = {0.05, 0.1, 0.15, 0.1, 0.1};
	selectivity.push_back(element1);
	selectivity.push_back(element2);
	selectivity.push_back(element3);

	TupleSchema fact_schema;
	fact_schema.add_attribute(Attribute("key_1",INT32,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("key_2",STRING,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("key_3",INT64,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("key_4",STRING,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("key_5",STRING,NOT_NULLABLE));
	fact_schema.add_attribute(Attribute("agg",INT32,NOT_NULLABLE));

	vector<TupleSchema> dimension_schemas;
	TupleSchema dimension1_schema, dimension2_schema, dimension3_schema, dimension4_schema, dimension5_schema;
	dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));
	dimension2_schema.add_attribute(Attribute("dimension_2",INT32,NULLABLE));
	dimension3_schema.add_attribute(Attribute("dimension_3",DOUBLE,NULLABLE));
	dimension4_schema.add_attribute(Attribute("dimension_4",INT64,NULLABLE));
	dimension5_schema.add_attribute(Attribute("dimension_5",DOUBLE,NULLABLE));
	dimension_schemas.push_back(dimension1_schema);
	dimension_schemas.push_back(dimension2_schema);
	dimension_schemas.push_back(dimension3_schema);
	dimension_schemas.push_back(dimension4_schema);
	dimension_schemas.push_back(dimension5_schema);

	CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
	ag_gr_p->add(ProjectNamedAttribute("key_1"));
	ag_gr_p->add(ProjectNamedAttribute("key_2"));
	ag_gr_p->add(ProjectNamedAttribute("key_3"));
	ag_gr_p->add(ProjectNamedAttribute("key_4"));
	ag_gr_p->add(ProjectNamedAttribute("key_5"));

	TestDataGenerater tg(fact_schema,
			dimension_schemas,
			row_group_size,
			row_group_count,
			query_count,
			fact_column_count,
			key_column_count,
			ag_gr_p,
			&column_width,
			&visit_locality,
			&selectivity);
	const TestDataSet* test_data_set = tg.GenerateDataSet();

	int memory_limit = 1024 * 500 * 5;
	//vector<double> init_visit_locality = {0.2, 0.2, 0.2, 0.2, 0.2};
	//vector<double> init_visit_locality = {0.05, 0.02, 0.1, 0.8, 0.03};
/*
	vector<double> init_visit_locality = {
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01,
		0.01, 0.01, 0.01, 0.01, 0.01};
*/
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

	vector<int> key_column_width = {4, 48, 8, 48, 80};
	Optimizer* optimizer = new Optimizer(row_group_count,
			key_column_count,
			query_count,
			memory_limit,
			&key_column_width,
			&query_proportion,
			&init_visit_locality,
			&selectivity);
	const vector<vector<bool>>* storage_map = optimizer->optimized_storage_map();

	//std::cout << "optimizer over" << std::endl;

	const View& fact_view = test_data_set->FactView(*storage_map);
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
	
	
	struct timespec begin, end;
	clock_gettime(CLOCK_REALTIME, &begin);

	vector<double> query_choice = {0.2 , 0.4, 1};
	for(int i = 0; i < 100; i++) {
		vector<vector<Operation*>> lhs_children_vector;
		for(int query_id_it = 0; query_id_it < query_count; query_id_it++) {
			vector<Operation*> lhs_children;
			for(int key_id = 0; key_id < dimension_schemas.size(); key_id++) {
				lhs_children.push_back(ScanView(*dimension_views_vector[query_id_it][key_id]));
				//std::cout<<"read dimension table end "<<i<<std::endl;
			}
			lhs_children_vector.push_back(lhs_children);
		}
		Operation* fact_operation = ScanView(fact_view);
		double poss = (double)i / 100;
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
		//View rv(resv.view());
		//ViewPrinter view_printer;
		//view_printer.AppendViewToStream(rv, &cout);
	}

	clock_gettime(CLOCK_REALTIME, &end);

	std::cout << "time: "	<< get_time(begin, end) << " ms." << std::endl << std::endl;
	std::cout<<">>>>>>>>>>>>calculate end<<<<<<<<<<<<<<"<<std::endl;
	fact_view.ColumnPieceVisitTimes(ag_gr_p);
	return 0;
}

