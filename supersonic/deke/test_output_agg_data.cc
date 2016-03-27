#include <iostream>

#include "supersonic/supersonic.h"

#include<memory>
#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/utils/basictypes.h"


using namespace std;

using namespace supersonic;

int main()
{

	const unsigned size = rowGroupSize*10 + 200;
	int64 *fact_group_by=(int64 *)malloc(size*sizeof(int64));
	int64 *fact_aggregate= (int64 *)malloc(size * sizeof(int64));
	double *dimension_1=(double *)malloc(1024*sizeof(double));
	double *dimension_2=(double *)malloc(1024*sizeof(double));
	for(int i=0;i<size;i++)
	{
		fact_group_by[i]=i % 1024;
		fact_aggregate[i]=(i %1024)*10;
		dimension_1[i%1024]=(i%1024)*0.1;
		dimension_2[i%1024]=(i%1024)*0.01;
	}

	TupleSchema fact_schema;
	fact_schema.add_attribute(Attribute("key_1",INT64,NULLABLE));
	fact_schema.add_attribute(Attribute("key_2",INT64,NULLABLE));
	fact_schema.add_attribute(Attribute("agg",INT64,NULLABLE));

	TupleSchema dimension1_schema;
	dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));

	TupleSchema dimension2_schema;
	dimension2_schema.add_attribute(Attribute("dimension_2",DOUBLE,NULLABLE));

	View fact_table(fact_schema),dimension1(dimension1_schema),dimension2(dimension2_schema);

	bool* fact_is_null = new bool[size];
	memset(fact_is_null,false,size);
	bool* dimension_is_null = new bool[size];
	memset(dimension_is_null,false,size);
	fact_table.set_row_count(size);
	fact_table.mutable_column(0)->Reset(fact_group_by,fact_is_null,size);
	fact_table.mutable_column(1)->Reset(fact_group_by,fact_is_null,size);
	fact_table.mutable_column(2)->Reset(fact_aggregate,fact_is_null,size);

	dimension1.set_row_count(rowGroupSize);
	dimension2.set_row_count(rowGroupSize);
	dimension1.mutable_column(0)->Reset(dimension_1, dimension_is_null,rowGroupSize);
	dimension2.mutable_column(0)->Reset(dimension_2, dimension_is_null,rowGroupSize);

	cout<<">>>>start<<<<"<<endl;
	File* fp=File::OpenOrDie("./fact_table_data","w");
	scoped_ptr<Sink> sink(FileOutput(fp, TAKE_OWNERSHIP));
	cout<<sink->Write(fact_table).is_failure()<<endl;
	sink->Finalize();
	//fp->Close();
	cout<<">>>>end fact_table_data<<<<"<<endl;
	cout<<">>>>start<<<<"<<endl;
	File* fp1=File::OpenOrDie("./dimension1_table_data","w");
	scoped_ptr<Sink> sink1(FileOutput(fp1, TAKE_OWNERSHIP));
	cout<<sink1->Write(dimension1).is_failure()<<endl;
	sink1->Finalize();
	//fp1->Close();
	cout<<">>>>end dimension1_table_data<<<<"<<endl;
	cout<<">>>>start<<<<"<<endl;
	File* fp2=File::OpenOrDie("./dimension2_table_data","w");
	scoped_ptr<Sink> sink2(FileOutput(fp2, TAKE_OWNERSHIP));
	cout<<sink2->Write(dimension2).is_failure()<<endl;
	sink2->Finalize();
	//fp2->Close();
	cout<<">>>>end dimension2_table_data<<<<"<<endl;

	return 0;
}



