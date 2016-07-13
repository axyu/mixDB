#ifndef INCLUDE_SSB_Q4_2_H
#define INCLUDE_SSB_Q4_2_H

#include <cstdlib>
#include <ctime>
#include "supersonic/supersonic.h"
#include "string.h"
#include "deke/include/ssb.h"

namespace supersonic {

class SSB_Q4_2_Schema {
public:
	static TupleSchema customer() {
		TupleSchema customer_schema;
		customer_schema.add_attribute(Attribute("C_REGION", STRING, NULLABLE));
		return customer_schema;
	}
	static TupleSchema supplier() {
		TupleSchema supplier_schema;
		supplier_schema.add_attribute(Attribute("S_NATION", STRING, NULLABLE));
		return supplier_schema;
	}
	static TupleSchema part() {
		TupleSchema supplier_schema;
		supplier_schema.add_attribute(Attribute("P_CATEGORY", STRING, NULLABLE));
		return supplier_schema;
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
		lineorder_schema.add_attribute(Attribute("LO_AGGVALUE", INT64, NULLABLE));
		return lineorder_schema;
	}
};

class Table2Data_Q4_2 {
public:
	static void Customer(std::string tbl_path, std::string binary_path){
		TupleSchema customer_schema = SSB_Q4_2_Schema::customer();
		Table customer(customer_schema, HeapBufferAllocator::Get());
		FILE* fp = fopen(tbl_path.c_str(),"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(fgets(buffer, 8192, fp)) {
			customer.AddRow();
			char *p;
			p = strtok(buffer, "|");
			bool not_null = true;
			for(int i = 0; i < 8 && p; i++) {
				if(i == 5) {
					customer.Set<STRING>(0, row_id, std::string(p));
				}
				if(i == 5) {
					// normal ssb test
					not_null = (std::string(p) == "AMERICA");
				}
				p = strtok(NULL, "|");
			}
			if(!not_null) customer.SetNull(0, row_id);
			row_id ++;
		}
		fclose(fp); 
		File* fpw=File::OpenOrDie(binary_path, "w");
		scoped_ptr<Sink> sink(FileOutput(fpw, TAKE_OWNERSHIP));
		sink->Write(customer.view()).is_failure();
		sink->Finalize();
		ViewPrinter vp;
		vp.AppendViewToStream(customer.view(), &std::cout);
	}
	static void Supplier(std::string tbl_path, std::string binary_path){
		TupleSchema supplier_schema = SSB_Q4_2_Schema::supplier();
		Table supplier(supplier_schema, HeapBufferAllocator::Get());
		FILE* fp = fopen(tbl_path.c_str(),"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(fgets(buffer, 8192, fp)) {
			supplier.AddRow();
			char *p;
			p = strtok(buffer, "|");
			bool not_null = true;
			for(int i = 0; i < 7 && p; i++) {
				if(i == 4) {
					supplier.Set<STRING>(0, row_id, std::string(p));
				}
				// normal ssb test
				if(i == 5) {
					not_null = (std::string(p) == "AMERICA");
				}
				p = strtok(NULL, "|");
			}
			if(!not_null) supplier.SetNull(0, row_id);
			row_id ++;
		}
		fclose(fp);
		File* fpw=File::OpenOrDie(binary_path, "w");
		scoped_ptr<Sink> sink(FileOutput(fpw, TAKE_OWNERSHIP));
		sink->Write(supplier.view()).is_failure();
		sink->Finalize();
		ViewPrinter vp;
		vp.AppendViewToStream(supplier.view(), &std::cout);
	}
	static void Part(std::string tbl_path, std::string binary_path){
		TupleSchema supplier_schema = SSB_Q4_2_Schema::part();
		Table supplier(supplier_schema, HeapBufferAllocator::Get());
		FILE* fp = fopen(tbl_path.c_str(),"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(fgets(buffer, 8192, fp)) {
			supplier.AddRow();
			char *p;
			p = strtok(buffer, "|");
			bool not_null = true;
			for(int i = 0; i < 7 && p; i++) {
				if(i == 3) {
					supplier.Set<STRING>(0, row_id, std::string(p));
				}
				// normal ssb test
				if(i == 2) {
					not_null = (std::string(p) == "MFGR#1" || std::string(p) == "MFGR#2");
				}
				p = strtok(NULL, "|");
			}
			if(!not_null) supplier.SetNull(0, row_id);
			row_id ++;
		}
		fclose(fp);
		File* fpw=File::OpenOrDie(binary_path, "w");
		scoped_ptr<Sink> sink(FileOutput(fpw, TAKE_OWNERSHIP));
		sink->Write(supplier.view()).is_failure();
		sink->Finalize();
		ViewPrinter vp;
		vp.AppendViewToStream(supplier.view(), &std::cout);
	}

	static void Date(std::string tbl_path, std::string binary_path){
		TupleSchema date_schema = SSB_Q4_2_Schema::date();
		Table date(date_schema, HeapBufferAllocator::Get());
		FILE* fp = fopen(tbl_path.c_str(),"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(fgets(buffer, 8192, fp)) {
			date.AddRow();
			char *p;
			p = strtok(buffer, "|");
			bool not_null = true;
			for(int i = 0; i < 18 && p; i++) {
				if(i == 4) {
					date.Set<INT64>(0, row_id, atoi(p));
				}
				// normal ssb test
				if(i == 4) {
					not_null = (atoi(p) == 1997 || atoi(p) == 1998);
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
		TupleSchema lineorder_schema = SSB_Q4_2_Schema::lineorder();
		Table lineorder(lineorder_schema, HeapBufferAllocator::Get());
		FILE* fp = fopen(tbl_path.c_str(),"r");
		char buffer[8192];
		rowid_t row_id = 0;
		while(fgets(buffer, 8192, fp)) {
			lineorder.AddRow();
			char *p;
			long agg_value = 0;
			p = strtok(buffer, "|");
			for(int i = 0; i < lineorder_schema.attribute_count() && p; i++) {
				if(i == 5 || i == 15) {
					lineorder.Set<INT64>(i, row_id, get_days(19920101, atoi(p)));
				} else {
					if(lineorder_schema.attribute(i).type() == INT64) {
						if(i == 12) agg_value = atoi(p);
						if(i == 13) agg_value -= atoi(p);
						lineorder.Set<INT64>(i, row_id, atoi(p));
					} else {
						lineorder.Set<STRING>(i, row_id, std::string(p));
					}
				}
				p = strtok(NULL, "|");
			}
			lineorder.Set<INT64>(lineorder_schema.attribute_count() - 1, row_id, agg_value);
			row_id ++;
		}
		fclose(fp);
		File* fpw=File::OpenOrDie(binary_path, "w");
		scoped_ptr<Sink> sink(FileOutput(fpw, TAKE_OWNERSHIP));
		sink->Write(lineorder.view()).is_failure();
		sink->Finalize();
		//ViewPrinter vp;
		//vp.AppendViewToStream(lineorder.view(), &std::cout);
	}
};
}


#endif
