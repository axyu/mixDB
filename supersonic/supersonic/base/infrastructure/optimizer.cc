// axyu add
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/optimizer.h"

namespace supersonic {

FailureOrVoid Optimizer::DoOptimize() {
	if(optimized_) return Success();
	CHECK_EQ(column_count_, column_width_->size());
	CHECK_EQ(query_count_, query_proportion_->size());
	CHECK_EQ(row_group_count_, visit_locality_->size());
	CHECK_EQ(query_count_, selectivity_->size());
	CHECK_EQ(column_count_, selectivity_->at(0).size());
	if(!filled_) FillKnapsack();
	int weight_limit_ = memory_limit_ / kRowGroupSize;
	vector<double> value(weight_limit_ + 1, 0.0);
	vector<vector<int>> item_choice(weight_limit_ + 1, vector<int>(row_group_count_, -1));
	for(int i = 0; i < row_group_count_; i++) {
		for(int v = weight_limit_; v >= 0; v--) {
			for(int item_id = 0; item_id < knapsack_.size(); item_id++) {
				double curr_value = 0.0;
				if(v >= knapsack_[item_id].width_ && 
					 (curr_value = value[v - knapsack_[item_id].width_] + 
					 	knapsack_[item_id].value_ * visit_locality_->at(i)) 
						> value[v]) {
					value[v] = curr_value;
					item_choice[v] = item_choice[v - knapsack_[item_id].width_];
					item_choice[v][i] = item_id;
				}
			}
		}
	}
	// Generate the storage map.
	for(int i = 0; i < row_group_count_; i++) {
		if(item_choice[weight_limit_][i] == -1)
			storage_map_[i] = vector<bool>(column_count_, false);
		else
			storage_map_[i] = knapsack_[item_choice[weight_limit_][i]].in_memory_;
	}
	// Generate the filter order.
	for(int query_id = 0; query_id < query_count_; query_id++) {
		for(int row_group_id = 0; row_group_id < row_group_count_; row_group_id ++) {
			int pointer = 0;
			for(int i = 0; i < column_count_; i++) {
				if(storage_map_[row_group_id][value_order_[query_id][i]]) 
					filter_order_[query_id][row_group_id][pointer++] = value_order_[query_id][i];
			}
			for(int i = 0; i < column_count_; i++) {
				if(!storage_map_[row_group_id][value_order_[query_id][i]]) 
					filter_order_[query_id][row_group_id][pointer++] = value_order_[query_id][i];
			}
		}
	}
	optimized_ = true;
	return Success();
}

void Optimizer::QuickSort(int query_id, vector<int>& v, int left, int right) {
	if(left < right) {
		int temp = v[left];
		int i = left, j = right;
		// selectivity of each column should not be zero.
		double x = column_width_->at(v[left]) / selectivity_->at(query_id)[v[left]];
		while(i < j) {
			while(i < j && column_width_->at(v[j]) / selectivity_->at(query_id)[v[j]] <= x)
				j--;
			if(i < j)
				v[i++] = v[j];
			while(i < j && column_width_->at(v[i]) / selectivity_->at(query_id)[v[i]] > x)
				i++;
			if(i < j)
				v[j--] = v[i];
		}
		v[i] = temp;
		QuickSort(query_id, v, left, i - 1);
		QuickSort(query_id, v, i + 1, right);
	}
}

void Optimizer::UpdateValueOrder() {
	for(int i = 0; i < query_count_; i++) {
		for(int j = 0; j < column_count_; j++) {
			value_order_[i][j] = j;
		}
		QuickSort(i, value_order_[i], 0, column_count_ - 1);
	}
}

double Optimizer::ioCostOfOneQuery(int query_id, const vector<bool>& in_memory) {
	const vector<double>* query_selectivity = &selectivity_->at(query_id);
	double IO = 0.0;
	double accumulate_selectivity = 1.0;
	for(int i = 0; i < column_count_; i++) {
		if(in_memory[value_order_[query_id][i]]) accumulate_selectivity *= query_selectivity->at(value_order_[query_id][i]);
	}
	double disk_selectivity = 1.0;
	for(int i = 0; i < column_count_; i++) {
		if(!in_memory[value_order_[query_id][i]]) {
			IO += column_width_->at(value_order_[query_id][i]) * accumulate_selectivity * disk_selectivity;
			disk_selectivity *= query_selectivity->at(value_order_[query_id][i]);
		}
	}
	return IO;
}

double Optimizer::valueOfOneQuery(int query_id, const vector<bool>& in_memory) {
	vector<bool> all_not_in_memory(column_count_, false);
	return ioCostOfOneQuery(query_id, all_not_in_memory) - ioCostOfOneQuery(query_id, in_memory);
}

double Optimizer::value(const vector<bool>& in_memory) {
	double result_value = 0.0;
	for(int i = 0; i < query_count_; i++) {
		result_value += valueOfOneQuery(i, in_memory) * query_proportion_->at(i);
	}
	return result_value;
}

// The subset sum problem.
void Optimizer::SubsetSumRecursive(vector<int> column_width, vector<int> position, int target, vector<int> partial, vector<int> partial_position, double& max_value, vector<bool>& in_memory) {
	int s = 0;
	for(vector<int>::const_iterator cit = partial.begin(); cit != partial.end(); cit++) {
		s += *cit;
	}
	if(s == target)
	{
		vector<bool> temp_in_memory(column_count_, false);
		for(int i = 0; i < partial_position.size(); i++) {
			temp_in_memory[partial_position[i]] = true;
		}
		double curr_value = value(temp_in_memory);
		if(curr_value > max_value) {
			max_value = curr_value;
			in_memory = temp_in_memory;
		}
	}
	if(s >= target) return;
	int n;
	for (int ai = 0; ai < column_width.size(); ai++) {
		n = column_width[ai];
		vector<int> remaining;
		vector<int> remaining_position;
		for (int aj = ai; aj < column_width.size(); aj++) {
			if(aj == ai) continue;
			remaining.push_back(column_width[aj]);
			remaining_position.push_back(position[aj]);
		}
		vector<int> partial_rec = partial;
		vector<int> partial_rec_position = partial_position;
		partial_rec.push_back(n);
		partial_rec_position.push_back(position[ai]);
		SubsetSumRecursive(remaining, remaining_position, target, partial_rec, partial_rec_position, max_value, in_memory);
	}
}

// Fill the knapsack with in-memory's columns vector and its width, value.
void Optimizer::FillKnapsack() {
	int column_width_sum = 0;
	for(int i = 0; i < column_count_; i++) {
		column_width_sum += column_width_->at(i);
	}
	for(int i = 1; i <= column_width_sum; i++) {
		double value = 0.0;
		vector<bool> in_memory(column_count_, false);
		vector<int> position;
		for(int i = 0; i < column_count_; i++) {
			position.push_back(i);
		}
		SubsetSumRecursive(*column_width_, position, i, vector<int>(), vector<int>(), value, in_memory);
		if(value > 0.0) {
			knapsack_.push_back(Items(i, in_memory, value)) ;
		}
	}
	filled_ = true;
}

// 
} // namespace supersonic
