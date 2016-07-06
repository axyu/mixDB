#ifndef INCLUDE_CAL_IO_H
#define INCLUDE_CAL_IO_H
#include <supersonic/supersonic.h>
#include <vector>
namespace supersonic {
int calculate_io(vector<vector<rowcount_t>>& visit_freq, vector<vector<bool>>& storage_map, vector<int> column_width) {
	CHECK_EQ(visit_freq.size(), column_width.size());
	CHECK_EQ(storage_map[0].size(), column_width.size());
	CHECK_EQ(visit_freq[0].size(), storage_map.size());
	int result = 0;
	for(int i = 0; i < storage_map.size(); i++) {
		for(int j = 0; j < storage_map[i].size(); j++) {
			if(!storage_map[i][j])
				result += visit_freq[j][i] * column_width[j];
		}
	}
	return result;
}
}
#endif
