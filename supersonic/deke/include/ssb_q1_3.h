#ifndef INCLUDE_SSB_H
#define INCLUDE_SSB_H

#include <cstdlib>
#include <ctime>
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

class SSB_Q1_3_Schema {
public:
	static TupleSchema groupkey() {
		TupleSchema date_schema;
		date_schema.add_attribute(Attribute("G_KEY", INT32, NULLABLE));
		return date_schema;
	}
	static TupleSchema date() {
		TupleSchema date_schema;
		date_schema.add_attribute(Attribute("D_YEAR", INT64, NULLABLE));
		return date_schema;
	}
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
		lineorder_schema.add_attribute(Attribute("LO_GROUPVALUE", INT64, NULLABLE));
		return lineorder_schema;
	}
};

class Table2Data_Q1_3 {
public:
	static void Date(std::string tbl_path, std::string binary_path){
		TupleSchema date_schema = SSB_Q1_3_Schema::date();
		Table date(date_schema, HeapBufferAllocator::Get());
		FILE* fp = fopen(tbl_path.c_str(),"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(fgets(buffer, 8192, fp)) {
			date.AddRow();
			char *p;
			p = strtok(buffer, "|");
			bool not_null = true;
			for(int i = 0; i < 8 && p; i++) {
				if(i == 4) {
					date.Set<INT64>(0, row_id, atoi(p));
				}
				if(i == 4) {
					not_null = (atoi(p) == 1993);
				}
				if(i == 11) {
					not_null = (atoi(p) == 6);
				}
				p = strtok(NULL, "|");
			}
			if(!not_null) date.SetNull(0, row_id);
			row_id ++;
		}
		fclose(fp);
		File* fpw=File::OpenOrDie(binary_path, "w");
		scoped_ptr<Sink> sink(FileOutput(fpw, TAKE_OWNERSHIP));
		sink->Write(date.view()).is_failure();
		sink->Finalize();
		ViewPrinter vp;
		vp.AppendViewToStream(date.view(), &std::cout);
	}

	static void Lineorder(std::string tbl_path, std::string binary_path){
		TupleSchema lineorder_schema = SSB_Q1_3_Schema::lineorder();
		TupleSchema group_key = SSB_Q1_3_Schema::groupkey();
		Table lineorder(lineorder_schema, HeapBufferAllocator::Get());
		Table group(group_key, HeapBufferAllocator::Get());
		FILE* fp = fopen(tbl_path.c_str(),"r");
		char buffer[8192];
		rowid_t row_id = 0;
		bool not_null = true;
		while(fgets(buffer, 8192, fp)) {
			lineorder.AddRow();
			group.AddRow();
			group.Set<INT32>(0, row_id, 1);
			char *p;
			p = strtok(buffer, "|");
			long extendedprice = -1;
			long discount = -1;
			for(int i = 0; i < lineorder_schema.attribute_count() && p; i++) {
				if(i == 5 || i == 15) {
					lineorder.Set<INT64>(i, row_id, get_days(19920101, atoi(p)));
				} else {
					if(lineorder_schema.attribute(i).type() == INT64) {
						if(i == 11 && !(atoi(p) >= 5 && atoi(p) <= 7)) not_null = false;//lo_discount
						if(i == 8 && !(atoi(p) >= 26 && atoi(p) <= 35)) not_null = false;//lo_quantity
						if(i == 9) extendedprice = atoi(p);
						if(i == 11) discount = atoi(p);
						lineorder.Set<INT64>(i, row_id, atoi(p));
					} else {
						lineorder.Set<STRING>(i, row_id, std::string(p));
					}
				}
				p = strtok(NULL, "|");
			}
			lineorder.Set<INT64>(lineorder_schema.attribute_count()-1, row_id, extendedprice * discount);
			if(!not_null) group.SetNull(0,row_id);
			row_id ++;
		}
		fclose(fp);
		File* fpw=File::OpenOrDie(binary_path, "w");
		scoped_ptr<Sink> sink(FileOutput(fpw, TAKE_OWNERSHIP));
		sink->Write(lineorder.view()).is_failure();
		sink->Finalize();
	
		File* key_fpw=File::OpenOrDie(binary_path+".key", "w");
		scoped_ptr<Sink> key_sink(FileOutput(key_fpw, TAKE_OWNERSHIP));
		key_sink->Write(group.view()).is_failure();
		key_sink->Finalize();
		//ViewPrinter vp;
		//vp.AppendViewToStream(lineorder.view(), &std::cout);
	}
};
}


#endif
