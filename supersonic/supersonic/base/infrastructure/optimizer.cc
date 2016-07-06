// axyu add
#include "supersonic/base/exception/exception_macros.h"
#include "supersonic/base/infrastructure/optimizer.h"
#include <iostream>

namespace supersonic {

	vector<vector<bool>> Optimizer::CacheHottestGroup(const vector<vector<rowcount_t>>& visit_freq) {
		CHECK_EQ(visit_freq.size(), column_count_);
		CHECK_EQ(visit_freq[0].size(), row_group_count_);
		vector<RowGroup> row_group;
		for(int i = 0; i < column_count_; i++) {
			for(int j = 0; j < row_group_count_; j++) {
				row_group.push_back(RowGroup(j, i, column_width_->at(i), visit_freq[i][j]));
			}
		}
		for(int i = 0; i < row_group.size(); i++) {
			for(int j = i; j < row_group.size(); j++) {
				if(row_group[j] > row_group[i]) {
					RowGroup temp = row_group[j];
					row_group[j] = row_group[i];
					row_group[i] = temp;
				}
			}
		}
		vector<vector<bool>> result_storage_map(row_group_count_, vector<bool>(column_count_, false));
		int accumulate_width = 0;
		for(int i = 0; i < row_group.size(); i++) {
			accumulate_width += row_group[i].width_;
			if(accumulate_width > memory_limit_ / kRowGroupSize) {
				return result_storage_map;
			}
			result_storage_map[row_group[i].group_id_][row_group[i].column_id_] = true;
		}
		return result_storage_map;
	}

	FailureOrVoid Optimizer::DoOptimize() {
		if(optimized_) return Success();

		CHECK_EQ(column_count_, column_width_->size());
		CHECK_EQ(query_count_, query_proportion_->size());
		CHECK_EQ(column_count_, visit_locality_->size());
		CHECK_EQ(query_count_, selectivity_->size());
		CHECK_EQ(column_count_, selectivity_->at(0).size());

		
		if(!filled_) FillKnapsack();
		// std::cout << " end subset " << std::endl;
		int weight_limit_ = memory_limit_ / kRowGroupSize;
		vector<double> value(weight_limit_ + 1, 0.0);
		vector<vector<int>> item_choice(weight_limit_ + 1, vector<int>(row_group_count_, -1));
		for(int i = 0; i < row_group_count_; i++) {
			for(int v = weight_limit_; v >= 0; v--) {
				for(int item_id = 0; item_id < knapsack_[i].size(); item_id++) {
					// std::cout << " item_id = " << item_id << std::endl;
					double curr_value = 0.0;
					if(v >= knapsack_[i][item_id].width_ && 
							(curr_value = value[v - knapsack_[i][item_id].width_] + 
							 knapsack_[i][item_id].value_) > value[v]) {
						value[v] = curr_value;
						item_choice[v] = item_choice[v - knapsack_[i][item_id].width_];
						item_choice[v][i] = item_id;
					}
				}
			}
		}
		// std::cout << " end knapsack " << std::endl;
		// Generate the storage map.
		for(int i = 0; i < row_group_count_; i++) {
			if(item_choice[weight_limit_][i] == -1)
				storage_map_[i] = vector<bool>(column_count_, false);
			else {
				storage_map_[i] = knapsack_[i][item_choice[weight_limit_][i]].in_memory_;
				//std::cout << "width: " << knapsack_[i][item_choice[weight_limit_][i]].width_ << std::endl;
			}

		}
		
		// build filter_index
		vector<vector<int>> filter_index(selectivity_->size(), vector<int>(selectivity_->at(0).size(), 0));
		for(int i = 0; i < selectivity_->size(); i++) {
			for(int j = 0; j < selectivity_->at(0).size(); j++) {
				if(selectivity_->at(i)[j] >= 0.0) {
					filter_index[i][j] = (j == 0) ? 0 : (filter_index[i][j - 1] + 1);
				}
				else {
					filter_index[i][j] = (j == 0) ? -1 : (filter_index[i][j - 1]);
				}
			}
			
		}

		// Generate the filter order.
		for(int query_id = 0; query_id < query_count_; query_id++) {
			for(int row_group_id = 0; row_group_id < row_group_count_; row_group_id ++) {
				int pointer = 0;
				for(int i = 0; i < column_count_; i++) {
					if(storage_map_[row_group_id][value_order_[query_id][row_group_id][i]]) {
						if(selectivity_->at(query_id)[value_order_[query_id][row_group_id][i]] >= 0.0)
							filter_order_[query_id][row_group_id].push_back(filter_index[query_id][value_order_[query_id][row_group_id][i]]);
					}
				}
				for(int i = 0; i < column_count_; i++) {
					if(!storage_map_[row_group_id][value_order_[query_id][row_group_id][i]]) {
						if(selectivity_->at(query_id)[value_order_[query_id][row_group_id][i]] >= 0.0)
							filter_order_[query_id][row_group_id].push_back(filter_index[query_id][value_order_[query_id][row_group_id][i]]);
					}
				}
			}
		}
		optimized_ = true;
		return Success();
	}

	double Optimizer::selectivity(int query_id, int row_group_id, int column_id) {
		double result = 0.0;
		result = (selectivity_->at(query_id)[column_id]) * (visit_locality_->at(column_id)[row_group_id]) * row_group_count_;
		//std::cout << "visit_locality: " << visit_locality_->at(column_id)[row_group_id] << std::endl;
		//CHECK_LE(result, 1);
		//CHECK_GE(result, 0);
		return result;
	}

	void Optimizer::QuickSort(int query_id, int row_group_id, vector<int>& v, int left, int right) {
		if(left < right) {
			int temp = v[left];
			int i = left, j = right;
			// selectivity of each column should not be zero.
			double x = column_width_->at(v[left]) / (1 - selectivity(query_id, row_group_id, v[left]));
			while(i < j) {
				while(i < j && column_width_->at(v[j]) / (1 - selectivity(query_id, row_group_id, v[j])) >= x)
					j--;
				if(i < j)
					v[i++] = v[j];
				while(i < j && column_width_->at(v[i]) / (1 - selectivity(query_id, row_group_id, v[i])) < x)
					i++;
				if(i < j)
					v[j--] = v[i];
			}
			v[i] = temp;
			QuickSort(query_id, row_group_id, v, left, i - 1);
			QuickSort(query_id, row_group_id, v, i + 1, right);
		}
	}

	void Optimizer::UpdateValueOrder() {
		for(int i = 0; i < query_count_; i++) {
			for(int j = 0; j < row_group_count_; j++) {
				for(int k = 0; k < column_count_; k++) {
					value_order_[i][j][k] = k;
				}
				QuickSort(i, j, value_order_[i][j], 0, column_count_ - 1);
			}
		}
	}

	double Optimizer::ioCostOfOneQuery(int query_id, int row_group_id, const vector<bool>& in_memory) {
		double IO = 0.0;
		double accumulate_selectivity = 1.0;
		for(int i = 0; i < column_count_; i++) {
			if(selectivity_->at(query_id)[value_order_[query_id][row_group_id][i]] >= 0.0 && in_memory[value_order_[query_id][row_group_id][i]]) accumulate_selectivity *= selectivity(query_id, row_group_id, value_order_[query_id][row_group_id][i]);
		}
		double disk_selectivity = 1.0;
		for(int i = 0; i < column_count_; i++) {
			if(selectivity_->at(query_id)[value_order_[query_id][row_group_id][i]] >= 0.0 && !in_memory[value_order_[query_id][row_group_id][i]]) {
				IO += column_width_->at(value_order_[query_id][row_group_id][i]) * accumulate_selectivity * disk_selectivity;
				disk_selectivity *= selectivity(query_id, row_group_id, value_order_[query_id][row_group_id][i]);
			}
		}
		return IO;
	}

	double Optimizer::valueOfOneQuery(int query_id, int row_group_id, const vector<bool>& in_memory) {
		vector<bool> all_not_in_memory(column_count_, false);
		return ioCostOfOneQuery(query_id, row_group_id, all_not_in_memory) - ioCostOfOneQuery(query_id, row_group_id, in_memory);
	}

	double Optimizer::value(int row_group_id, const vector<bool>& in_memory) {
		double result_value = 0.0;
		for(int i = 0; i < query_count_; i++) {
			result_value += valueOfOneQuery(i, row_group_id, in_memory) * query_proportion_->at(i);
		}
		return result_value;
	}

	// The subset sum problem.
	void Optimizer::SubsetSumRecursive(vector<int> column_width,
			vector<int> position, 
			int target, 
			vector<int> partial, 
			vector<int> partial_position, 
			vector<vector<bool>>& in_memory) {
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
			in_memory.push_back(temp_in_memory);
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
			SubsetSumRecursive(remaining, remaining_position, target, partial_rec, partial_rec_position, in_memory);
		}
	}

	// Fill the knapsack with in-memory's columns vector and its width, value.
	void Optimizer::FillKnapsack() {
		int column_width_sum = 0;
		for(int i = 0; i < column_count_; i++) {
			column_width_sum += column_width_->at(i);
		}
		vector<vector<vector<bool>>> subset_sum(column_width_sum, vector<vector<bool>>());
		for(int i = 0; i < column_width_sum; i++) {
			vector<int> position;
			for(int i = 0; i < column_count_; i++) {
				position.push_back(i);
			}
			SubsetSumRecursive(*column_width_, position, i+1, vector<int>(), vector<int>(), subset_sum[i]);
		}
		for(int row_group_id = 0; row_group_id < row_group_count_; row_group_id++) {
			vector<Items> group_knapsack;
			for(int i = 0; i < column_width_sum; i++) {
				double max_value = 0.0;
				vector<bool> in_memory(column_count_, false);
				for(int j = 0; j < subset_sum[i].size(); j++) {
					double temp_value = value(row_group_id, subset_sum[i][j]);
					if(temp_value > max_value) {
						max_value = temp_value;
						in_memory = subset_sum[i][j];
					}
				}
				if(max_value > 0.0) {
					group_knapsack.push_back(Items(i+1, in_memory, max_value));
					/*std::cout << "group_id: " << row_group_id
						  << " in_memory_width: " << i+1 
						  << " value: " << max_value << std::endl;
					*/
				}
			}
			knapsack_.push_back(group_knapsack);
		}
		filled_ = true;
	}

	// 
} // namespace supersonic
