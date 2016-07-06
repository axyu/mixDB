// axyu add
#ifndef SUPERSONIC_BASE_INFRASTRUCTURE_OPTIMIZER_H_
#define SUPERSONIC_BASE_INFRASTRUCTURE_OPTIMIZER_H_
#include <vector>
using std::vector;
#include "supersonic/base/exception/result.h"
#include "supersonic/base/infrastructure/types.h"
namespace supersonic {

	class Optimizer {
		public:
			static const rowcount_t kRowGroupSize = 300006;//6002;
			static const int kThreadCount = 4;
			vector<vector<bool>> CacheHottestGroup(const vector<vector<rowcount_t>>& visit_freq);
			// Constructor of the Optimizer class.
			Optimizer(int row_group_count,
					int column_count, 
					int query_count,
					int memory_limit,
					const vector<int>* column_width,
					const vector<double>* query_proportion,
					const vector<vector<double>>* visit_locality,
					const vector<vector<double>>* selectivity)
				: row_group_count_(row_group_count),
				column_count_(column_count),
				query_count_(query_count),
				memory_limit_(memory_limit),
				knapsack_(0, vector<Items>(0, Items())),
				filled_(false),
				column_width_(column_width),
				query_proportion_(query_proportion),
				visit_locality_(visit_locality),
				selectivity_(selectivity),
				optimized_(false),
				value_order_(query_count, vector<vector<int>>(row_group_count, vector<int>(column_count, -1))),
				filter_order_(query_count, vector<vector<int>>(row_group_count, vector<int>())),
				storage_map_(row_group_count, vector<bool>(column_count, false)) {
					UpdateValueOrder();
				}

			// Return the filter order by query id.
			const vector<vector<int>>* optimized_filter_order(int query_id) {
				if(!optimized_) 
					DoOptimize();
				return &filter_order_[query_id];

			}

			const vector<vector<bool>>* optimized_storage_map() {
				if(!optimized_) 
					DoOptimize();
				return &storage_map_;	
			}


			// Reset the configuration for a new optimization.
			FailureOrVoid ReConfigure(int row_group_count,
					int column_count,
					int query_count,
					int memory_limit,
					const vector<int>* column_width,
					const vector<double>* query_proportion,
					const vector<vector<double>>* visit_locality,
					const vector<vector<double>>* selectivity) {
				row_group_count_ = row_group_count;
				column_count_ = column_count;
				query_count_ = query_count;
				memory_limit_ = memory_limit;
				knapsack_.clear(); // Remove all items from the old backpack.
				filled_ = false;
				column_width_ = column_width;
				query_proportion_ = query_proportion;
				visit_locality_ = visit_locality;
				selectivity_ = selectivity;
				optimized_ = false;
				UpdateValueOrder();
				return Success();
			}

			~Optimizer() {}

		private:
			struct RowGroup {
				public:
					RowGroup():group_id_(-1), column_id_(-1), width_(-1), visit_freq_(-1) {}
					RowGroup(int group_id, int column_id, int width, rowcount_t visit_freq)
						: group_id_(group_id), column_id_(column_id), width_(width), visit_freq_(visit_freq) {}
					RowGroup(const RowGroup& rg) {
						group_id_ = rg.group_id_;
						column_id_ = rg.column_id_;
						width_ = rg.width_;
						visit_freq_ = rg.visit_freq_;
					}
					int group_id_;
					int column_id_;
					int width_;
					rowcount_t visit_freq_;

					bool operator > (const RowGroup& rg) {
					//	return ((double)visit_freq_ / (double)width_) > ((double)rg.visit_freq_ / (double)rg.width_);
						return visit_freq_ > rg.visit_freq_;
					}
			};
			// A class of the items in knapsack.
			struct Items {
				public:
					Items():width_(0), in_memory_(0, false), value_(0.0) {}
					Items(int width, vector<bool> in_memory, double value)
						: width_(width), in_memory_(in_memory), value_(value) {}

					int width_;
					vector<bool> in_memory_;
					double value_;
			};

			FailureOrVoid DoOptimize();
			double selectivity(int query_id, int row_group_id, int column_id);
			void QuickSort(int query_id, int row_group_id, vector<int>& v, int left, int right);
			void UpdateValueOrder();
			double ioCostOfOneQuery(int query_id, int row_group_id, const vector<bool>& in_memory);
			double valueOfOneQuery(int query_id, int row_group_id, const vector<bool>& in_memory);
			double value(int row_group_id, const vector<bool>& in_memory);
			void SubsetSumRecursive(vector<int> column_width,
					vector<int> position,
					int target, 
					vector<int> partial,
					vector<int> partial_position,
					vector<vector<bool>>& in_memory);
			void FillKnapsack();
			// The count of the row groups.
			int row_group_count_;
			// The count of the columns involoved in optimization.
			int column_count_;
			// The count of queries need to be executed.
			int query_count_;
			// The limit size of the main memory (Bytes).
			int memory_limit_;
			// Items in knapsack and their value.
			vector<vector<Items>> knapsack_;
			// Whether the knapsack has been filled.
			bool filled_;
			// Width of each column.
			const vector<int>* column_width_;
			// Proportion of each query.
			const vector<double>* query_proportion_;
			// Visit probability of each row group, no difference between columns.
			const vector<vector<double>>* visit_locality_;
			// Column's selectivity of each query.
			const vector<vector<double>>* selectivity_;
			// Whether the optimization has been done.
			bool optimized_;
			// The order of the value of each column for all queries.
			vector<vector<vector<int>>> value_order_;
			// Two results of the optimizer
			// 1. filter order : the order of filter in the fact table.
			// 2. stroage map : the storage way of each column's row groups
			//true->stored in main memory / false->stored in disk or ssd.
			vector<vector<vector<int>>> filter_order_;
			vector<vector<bool>> storage_map_;
	};
}	// namespace supersonic

#endif	// SUPERSONIC_BASE_INFRASTRUCTURE_OPTIMIZER_H_
