
#ifndef DEKE_VISIT_TIMES_TO_LOCALITY_H
#define DEKE_VISIT_TIMES_TO_LOCALITY_H

#include <vector>
#include "supersonic/supersonic.h"

#include <iostream>
using std::vector;

namespace supersonic {

vector<vector<double>> visit_times_to_locality(const vector<vector<rowcount_t>>& visit_times_vector) {
	int column_num = visit_times_vector.size();
	//std::cout << "column_num: " << column_num << std::endl;
	int row_num = visit_times_vector[column_num - 1].size();
	std::cout << "row_num: " << row_num << std::endl;
	vector<vector<double>> locality_vector(column_num, vector<double>(row_num, 0.0));
	vector<double> column_visit_times_sum(column_num, 0.0);
	for(int column_index = 0; column_index < column_num; column_index ++) {
		for(int row_index = 0; row_index < row_num; row_index ++) {
			column_visit_times_sum[column_index] += visit_times_vector[column_index][row_index];
			//std::cout << "visit_times_vector" << visit_times_vector[column_index][row_index] << std::endl;
		}
			//std::cout << "column_visit_sum" << column_visit_times_sum[column_index] << std::endl;
	}
	for(int column_index = 0; column_index < column_num; column_index ++) {
		for(int row_index = 0; row_index < row_num; row_index ++) {
			locality_vector[column_index][row_index] = 
				(double)visit_times_vector[column_index][row_index] / column_visit_times_sum[column_index];
			//std::cout << "locality: " << locality_vector[column_index][row_index] << std::endl;
		}
	}
	return locality_vector;
}

}

#endif


