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
	int memory_limit = 1024 * 5 * 20;
	vector<int> column_width = {4, 4, 8, 32, 128};
	vector<double> query_proportion = {0.2, 0.2, 0.6};
	vector<double> visit_locality = {0.05, 0.2, 0.1, 0.5, 0.15};
	vector<vector<double>> selectivity;
	vector<double> element1 = {0.2, 0.8, 0.8, 0.5, 0.1};
	vector<double> element2 = {0.2, 0.5, 0.6, 1, 0.2};
	vector<double> element3 = {0.4, 0.2, 0.8, 0.3, 0.6};
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
			for(int j = 0; j < column_count; j++) {
				cout << filter_order->at(i)[j] << " ";
			}
			cout << endl;
		}
	}
	return 0;
}
