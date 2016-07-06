
#ifndef DEKE_INCLUDE_GENERATE_TEST_LOCALITY_H
#define DEKE_INCLUDE_GENERATE_TEST_LOCALITY_H

#include "supersonic/supersonic.h"
#include <stdlib.h>
#include <vector>

using std::vector;

namespace supersonic {

const double minINC = 0.001;

double get_random() {
	return (rand() % 10)/(10 * 1.0) + (rand() % 10)/(100 * 1.0) + (rand() % 10)/(1000 * 1.0) + (rand() % 10)/(10000 * 1.0);
}

rowcount_t get_random_in(rowcount_t range) {
	return rand() % range;
}

// input example :
// row_group_count = 100
// range      =  0   20   40   60   80   100  
// probablity =   0.2  0.2  0.2  0.2  0.2
vector<double> generate_test_locality(rowcount_t row_group_count, vector<rowcount_t> range, vector<double> probablity) {
	CHECK(range.size() - 1 == probablity.size())<< "argument of generate_test_locality is illegal";
	vector<double> result(row_group_count, 0.0);
	for(double sum = 0; sum < 1.0; sum += minINC) {
		// get the range index
		double pro_sum = 0.0;
		int pro_index = 0;
		double pro_index_seed = get_random();
		for(; pro_index < probablity.size(); pro_index ++) {
			if(pro_index_seed >= pro_sum && pro_index_seed < probablity[pro_index] + pro_sum) { break; }
			pro_sum += probablity[pro_index];
		}
		
		// increase the locality of pro_index
		rowcount_t inc_index = range[pro_index] + get_random_in(range[pro_index + 1] - range[pro_index]);
		result[inc_index] += minINC;
	}
	return result;
}

template<typename type>
vector<vector<type>> extern_to_n_column(const int n, const vector<type> column_locality) {
	vector<vector<type>> result;
	for(int i = 0; i < n; i++) {
		result.push_back(column_locality);
	};
	return result;
}

}
#endif
