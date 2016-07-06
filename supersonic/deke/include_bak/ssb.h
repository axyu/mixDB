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
	static TupleSchema customer() {
		TupleSchema customer_schema;
		customer_schema.add_attribute(Attribute("C_CUSTKEY", INT64, NULLABLE));
		customer_schema.add_attribute(Attribute("C_NAME", STRING, NULLABLE));
		customer_schema.add_attribute(Attribute("C_ADDRESS", STRING, NULLABLE));
		customer_schema.add_attribute(Attribute("C_CITY", STRING, NULLABLE));
		customer_schema.add_attribute(Attribute("C_NATION", STRING, NULLABLE));
		customer_schema.add_attribute(Attribute("C_REGION", STRING, NULLABLE));
		customer_schema.add_attribute(Attribute("C_PHONE", STRING, NULLABLE));
		customer_schema.add_attribute(Attribute("C_MKTSEGMENT", STRING, NULLABLE));
		return customer_schema;
	}
	static TupleSchema part() {
		TupleSchema part_schema;
		part_schema.add_attribute(Attribute("P_PARTKEY", INT64, NULLABLE));
		part_schema.add_attribute(Attribute("P_NAME", STRING, NULLABLE));
		part_schema.add_attribute(Attribute("P_MFGR", STRING, NULLABLE));
		part_schema.add_attribute(Attribute("P_CATEGORY", STRING, NULLABLE));
		part_schema.add_attribute(Attribute("P_BRAND1", STRING, NULLABLE));
		part_schema.add_attribute(Attribute("P_COLOR", STRING, NULLABLE));
		part_schema.add_attribute(Attribute("P_TYPE", STRING, NULLABLE));
		part_schema.add_attribute(Attribute("P_SIZE", INT64, NULLABLE));
		part_schema.add_attribute(Attribute("P_CONTAINER", STRING, NULLABLE));
		return part_schema;
	}
	static TupleSchema supplier() {
		TupleSchema supplier_schema;
		supplier_schema.add_attribute(Attribute("S_SUPPKEY", INT64, NULLABLE));
		supplier_schema.add_attribute(Attribute("S_NAME", STRING, NULLABLE));
		supplier_schema.add_attribute(Attribute("S_ADDRESS", STRING, NULLABLE));
		supplier_schema.add_attribute(Attribute("S_CITY", STRING, NULLABLE));
		supplier_schema.add_attribute(Attribute("S_NATION", STRING, NULLABLE));
		supplier_schema.add_attribute(Attribute("S_REGION", STRING, NULLABLE));
		supplier_schema.add_attribute(Attribute("S_PHONE", STRING, NULLABLE));
		return supplier_schema;
	}
	static TupleSchema dwdate() {
		TupleSchema dwdate_schema;
		dwdate_schema.add_attribute(Attribute("D_DATEKEY", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_DATE", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_DAYOFWEEK", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_MONTH", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_YEAR", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_YEARMONTHNUM", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_YEARMONTH", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_DAYNUMINWEEK", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_DAYNUMINMONTH", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_DAYNUMINYEAR", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_MONTHNUMINYEAR", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_WEEKNUMINYEAR", INT64, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_SELLINGSEASON", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_LASTDAYINWEEKFL", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_LASTDAYINMONTHFL", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_HOLIDAYFL", STRING, NULLABLE));
		dwdate_schema.add_attribute(Attribute("D_WEEKDAYFL", STRING, NULLABLE));
		return dwdate_schema;
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
		return lineorder_schema;
	}
};

class Table2Data {
public:
	static void Customer(std::string tbl_path, std::string binary_path){
		TupleSchema customer_schema = SSBSchema::customer();
		Table customer(customer_schema, HeapBufferAllocator::Get());
		File *fp = File::OpenOrDie(tbl_path,"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(!fp->eof()) {
			fp->ReadLine(buffer, 8192);
			customer.AddRow();
			//buffer[strlen(buffer) - 1] = '\0';//what is the meaning?
			char *p;
			p = strtok(buffer, "|");
			for(int i = 0; i < customer_schema.attribute_count() && p; i++) {
				if( i == 0 ) {
					customer.Set<INT64>(i, row_id, atoi(p));
				} else {
					customer.Set<STRING>(i, row_id, std::string(p));
				}
				p = strtok(NULL, "|");
			}
			row_id ++;
		}
		fp->Close();
		ViewPrinter vp;
		vp.AppendViewToStream(customer.view(), &std::cout);
	}

	static void Part(std::string tbl_path, std::string binary_path){
		TupleSchema part_schema = SSBSchema::part();
		Table part(part_schema, HeapBufferAllocator::Get());
		File *fp = File::OpenOrDie(tbl_path,"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(!fp->eof()) {
			fp->ReadLine(buffer, 8192);
			part.AddRow();
			//buffer[strlen(buffer) - 1] = '\0';
			char *p;
			p = strtok(buffer, "|");
			for(int i = 0; i < part_schema.attribute_count() && p; i++) {
				if(part_schema.attribute(i).type() == INT64) {
					part.Set<INT64>(i, row_id, atoi(p));
				} else {
					part.Set<STRING>(i, row_id, std::string(p));
				}
				p = strtok(NULL, "|");
			}
			row_id ++;
		}
		fp->Close();
		ViewPrinter vp;
		vp.AppendViewToStream(part.view(), &std::cout);
	}

	static void Supplier(std::string tbl_path, std::string binary_path){
		TupleSchema supplier_schema = SSBSchema::supplier();
		Table supplier(supplier_schema, HeapBufferAllocator::Get());
		File *fp = File::OpenOrDie(tbl_path,"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(!fp->eof()) {
			fp->ReadLine(buffer, 8192);
			supplier.AddRow();
			//buffer[strlen(buffer) - 1] = '\0';
			char *p;
			p = strtok(buffer, "|");
			for(int i = 0; i < supplier_schema.attribute_count() && p; i++) {
				if(supplier_schema.attribute(i).type() == INT64) {
					supplier.Set<INT64>(i, row_id, atoi(p));
				} else {
					supplier.Set<STRING>(i, row_id, std::string(p));
				}
				p = strtok(NULL, "|");
			}
			row_id ++;
		}
		fp->Close();
		ViewPrinter vp;
		vp.AppendViewToStream(supplier.view(), &std::cout);
	}

	static void Dwdate(std::string tbl_path, std::string binary_path){
		TupleSchema dwdate_schema = SSBSchema::dwdate();
		Table dwdate(dwdate_schema, HeapBufferAllocator::Get());
		File *fp = File::OpenOrDie(tbl_path,"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(!fp->eof()) {
			fp->ReadLine(buffer, 8192);
			dwdate.AddRow();
			//buffer[strlen(buffer) - 1] = '\0';
			char *p;
			p = strtok(buffer, "|");
			for(int i = 0; i < dwdate_schema.attribute_count() && p; i++) {
				if(dwdate_schema.attribute(i).type() == INT64) {
					dwdate.Set<INT64>(i, row_id, atoi(p));
				} else {
					dwdate.Set<STRING>(i, row_id, std::string(p));
				}
				p = strtok(NULL, "|");
			}
			row_id ++;
		}
		fp->Close();
		ViewPrinter vp;
		vp.AppendViewToStream(dwdate.view(), &std::cout);
	}

	static void Lineorder(std::string tbl_path, std::string binary_path){
		TupleSchema lineorder_schema = SSBSchema::lineorder();
		Table lineorder(lineorder_schema, HeapBufferAllocator::Get());
		File *fp = File::OpenOrDie(tbl_path,"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(!fp->eof()) {
			fp->ReadLine(buffer, 8192);
			lineorder.AddRow();
			//buffer[strlen(buffer) - 1] = '\0';
			char *p;
			p = strtok(buffer, "|");
			for(int i = 0; i < lineorder_schema.attribute_count() && p; i++) {
				if(lineorder_schema.attribute(i).type() == INT64) {
					lineorder.Set<INT64>(i, row_id, atoi(p));
				} else {
					lineorder.Set<STRING>(i, row_id, std::string(p));
				}
				p = strtok(NULL, "|");
			}
			row_id ++;
		}
		fp->Close();
		ViewPrinter vp;
		vp.AppendViewToStream(lineorder.view(), &std::cout);
	}
};
}


#endif
