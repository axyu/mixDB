#ifndef INCLUDE_SSB_H
#define INCLUDE_SSB_H

#include "supersonic/supersonic.h"
#include "string.h"

namespace supersonic {
int get_days(int from, int to) {
	int year_from = from / 10000;
	int month_from = (from % 10000) / 100;
	int day_from = from % 100;

	int year_to = to / 10000;
	int month_to = (to % 10000) / 100;
	int day_to = to % 100;

	tm info_from={0};
	info_from.tm_year = year_from-1900;
	info_from.tm_mon = month_from-1;
	info_from.tm_mday = day_from;

	tm info_to={0};
	info_to.tm_year = year_to-1900;
	info_to.tm_mon = month_to-1;
	info_to.tm_mday = day_to;

	return ((int)mktime(&info_to) - (int)mktime(&info_from))/24/3600;
}


class SSBSchema {
public:
	static TupleSchema lineorder() {
		TupleSchema lineorder_schema;
		lineorder_schema.add_attribute(Attribute("LO_ORDERKEY", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_LINENUMBER", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_CUSTKEY", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_PARTKEY", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_SUPPKEY", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_ORDERDATE", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_ORDERPRIORITY", STRING, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_SHIPPRIORITY", STRING, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_QUANTITY", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_EXTENDEDPRICE", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_ORDERTOTALPRICE", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_DISCOUNT", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_REVENUE", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_SUPPLYCOST", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_TAX", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_COMMITDATE", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_SHIPMODE", STRING, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_AGGKEY", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_AGGQ1VALUE", INT64, NULLABLE));
		lineorder_schema.add_attribute(Attribute("LO_AGGQ4VALUE", INT64, NULLABLE));
		return lineorder_schema;
	}
};

class Table2Data {
public:
	static void Lineorder(std::string tbl_path, std::string binary_path);
};
}


#endif
