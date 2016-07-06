#include <iostream>
#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/test_data_generater.h"
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include <vector>
#include <sstream>
#include "deke/include/visit_times_to_locality.h"
#include "deke/include/generate_test_locality.h"

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
	int row_group_count = 500 ;
	int query_count = 3;
	int fact_column_count = 6;
	int key_column_count = 5;
	vector<int> column_width = {4, 40, 8, 48, 80, 4};
	vector<double> query_proportion = {0.2, 0.2, 0.6};
	vector<rowcount_t> range({0, row_group_count / (rowcount_t)3, (row_group_count / (rowcount_t)3)*(rowcount_t)2, row_group_count});
	vector<double> probablity({0.1,0.1,0.8});
	vector<vector<double>> visit_locality = extern_to_n_column<double>(5, 
				generate_test_locality(row_group_count, range, probablity));
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

	return 0;
}

