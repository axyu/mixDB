#include <iostream>
#include "supersonic/supersonic.h"
#include <vector>

using std::cout;
using std::endl;
using std::vector;
using supersonic::Optimizer;

int main() {
	int row_group_count = 5;
	int column_count = 5;
	int query_count = 3;
	int memory_limit = 1024 * (22 * 8 - 1);
	vector<int> column_width = {4, 24, 20, 8, 48};
	vector<double> query_proportion = {0.2, 0.2, 0.6};
	vector<vector<double>> visit_locality(5, {0.05, 0.2, 0.2, 0.3, 0.25});
	vector<vector<double>> selectivity;
	vector<double> element1 = {0.6, -1, 0.1, -1, 0.1};
	//vector<double> element1 = {0.6, 0.6, 0.1, 0.6, 0.1};
	//vector<double> element2 = {0.6, 0.5, 0.2, 0.5, 0.02};
	vector<double> element2 = {0.6, 0.5, -1, 0.5, 0.02};
	//vector<double> element3 = {0.6, 0.2, 0.2, 0.6, 0.01};
	vector<double> element3 = {0.6, -1, 0.2, 0.6, -1};
	selectivity.push_back(element1);
	selectivity.push_back(element2);
	selectivity.push_back(element3);
	Optimizer* optimizer = new Optimizer(row_group_count,
			column_count,
			query_count,
			memory_limit,
			&column_width,
			&query_proportion,
			&visit_locality,
			&selectivity);
	const vector<vector<bool>>* storage_map = optimizer->optimized_storage_map();
	cout << "storage map (1 is in-memory / 0 is out-of-memory)" << endl;
	for(int i = 0; i < row_group_count; i++) {
		for(int j = 0; j < column_count; j++) {
			cout << storage_map->at(i)[j] << " ";
		}
		cout << endl;
	}
	for(int t = 0; t < 3; t++) {
		const vector<vector<int>>* filter_order = optimizer->optimized_filter_order(t);
		cout << "query " << t << "'s result" << endl;
		for(int i = 0; i < row_group_count; i++) {
			cout << "row group " << i << "'s filter order is: ";
			for(int j = 0; j < filter_order->at(i).size(); j++) {
				cout << filter_order->at(i)[j] << " ";
			}
			cout << endl;
		}
	}
	/*

	   TupleSchema fact_schema;
	   fact_schema.add_attribute(Attribute("key_1",INT32,NULLABLE));
	   fact_schema.add_attribute(Attribute("key_2",STRING,NULLABLE));
	   fact_schema.add_attribute(Attribute("key_3",STRING,NULLABLE));
	   fact_schema.add_attribute(Attribute("key_4",INT64,NULLABLE));
	   fact_schema.add_attribute(Attribute("key_5",STRING,NULLABLE));
	   fact_schema.add_attribute(Attribute("agg",INT32,NULLABLE));

	   TupleSchema Q1_dimension1_schema, Q1_dimension2_schema, Q1_dimension3_schema, Q1_dimension4_schema, Q1_dimension5_schema;
	   Q1_dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));
	   Q1_dimension2_schema.add_attribute(Attribute("dimension_2",DOUBLE,NULLABLE));
	   Q1_dimension3_schema.add_attribute(Attribute("dimension_3",DOUBLE,NULLABLE));
	   Q1_dimension4_schema.add_attribute(Attribute("dimension_4",DOUBLE,NULLABLE));
	   Q1_dimension5_schema.add_attribute(Attribute("dimension_5",DOUBLE,NULLABLE));

	   TupleSchema Q2_dimension1_schema, Q2_dimension2_schema, Q2_dimension3_schema, Q2_dimension4_schema, Q2_dimension5_schema;
	   Q2_dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));
	   Q2_dimension2_schema.add_attribute(Attribute("dimension_2",DOUBLE,NULLABLE));
	   Q2_dimension3_schema.add_attribute(Attribute("dimension_3",DOUBLE,NULLABLE));
	   Q2_dimension4_schema.add_attribute(Attribute("dimension_4",DOUBLE,NULLABLE));
	   Q2_dimension5_schema.add_attribute(Attribute("dimension_5",DOUBLE,NULLABLE));

	   TupleSchema Q3_dimension1_schema, Q3_dimension2_schema, Q3_dimension3_schema, Q3_dimension4_schema, Q3_dimension5_schema;
	   Q3_dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));
	   Q3_dimension2_schema.add_attribute(Attribute("dimension_2",DOUBLE,NULLABLE));
	   Q3_dimension3_schema.add_attribute(Attribute("dimension_3",DOUBLE,NULLABLE));
	   Q3_dimension4_schema.add_attribute(Attribute("dimension_4",DOUBLE,NULLABLE));
	   Q3_dimension5_schema.add_attribute(Attribute("dimension_5",DOUBLE,NULLABLE));

	   AggregationSpecification *ag_spe = new AggregationSpecification();
	   ag_spe->AddAggregation(SUM,"agg","sum_agg");

	   CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
	   ag_gr_p->add(ProjectNamedAttribute("key_1"));
	   ag_gr_p->add(ProjectNamedAttribute("key_2"));
	   ag_gr_p->add(ProjectNamedAttribute("key_3"));
	   ag_gr_p->add(ProjectNamedAttribute("key_4"));
	   ag_gr_p->add(ProjectNamedAttribute("key_5"));

	   vector<Operation*> lhs_children;
	   lhs_children.push_back(ScanView(Q1_dim1.view()));
	   lhs_children.push_back(ScanView(Q1_dim2.view()));
	   lhs_children.push_back(ScanView(Q1_dim3.view()));
	   lhs_children.push_back(ScanView(Q1_dim4.view()));
	   lhs_children.push_back(ScanView(Q1_dim5.view()));

	   scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p,ag_spe,NULL,filter_order,lhs_children,ScanView(fact.view())));
	   scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

	   ResultView resv(ResultView(cr->Next(-1)));
	   View rv(resv.view());
	   */
	return 0;
}
