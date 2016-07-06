#include <iostream>
#include "supersonic/supersonic.h"
#include "supersonic/base/infrastructure/tuple_schema.h"
#include "supersonic/base/infrastructure/test_data_generater.h"
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include <vector>
#include <sstream>
#include "deke/include/visit_times_to_locality.h"
#include "deke/include/ssb_q4_2.h"

using std::cout;
using std::endl;
using std::vector;
using std::stringstream;
using namespace supersonic;

int get_time(struct timespec& begin, struct timespec& end) {
	return 1000 * (end.tv_sec - begin.tv_sec) + (end.tv_nsec - begin.tv_nsec) / 1000000;
}

int main(int argc, char* argv[]) {
	Table2Data_Q4_2::Customer("/home/fish/data/ssb/customer.tbl", "../storage/customer_q4_2.tbl");
	Table2Data_Q4_2::Supplier("/home/fish/data/ssb/supplier.tbl", "../storage/supplier_q4_2.tbl");
	Table2Data_Q4_2::Part("/home/fish/data/ssb/part.tbl", "../storage/part_q4_2.tbl");
	Table2Data_Q4_2::Date("/home/fish/data/ssb/date.tbl", "../storage/date_q4_2.tbl");
	Table2Data_Q4_2::Lineorder("/home/fish/data/ssb/lineorder.tbl", "../storage/lineorder_q4_2.tbl");
	return 0;
}
