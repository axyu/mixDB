#include <iostream>

#include "supersonic/supersonic.h"

#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/utils/basictypes.h"


using namespace std;

using namespace supersonic;

int main()
{

	const unsigned size = rowGroupSize*10 + 200;


	TupleSchema fact_schema;
	fact_schema.add_attribute(Attribute("key_1",INT64,NULLABLE));
	fact_schema.add_attribute(Attribute("key_2",INT64,NULLABLE));
	fact_schema.add_attribute(Attribute("agg",INT64,NULLABLE));

	TupleSchema dimension1_schema;
	dimension1_schema.add_attribute(Attribute("dimension_1",DOUBLE,NULLABLE));

	TupleSchema dimension2_schema;
	dimension2_schema.add_attribute(Attribute("dimension_2",DOUBLE,NULLABLE));


	Table fact(fact_schema,HeapBufferAllocator::Get());
	vector<rowcount_t> in_memory_row_capacity_vector;
	in_memory_row_capacity_vector.push_back(0);
	in_memory_row_capacity_vector.push_back(0);
	in_memory_row_capacity_vector.push_back(0);
	fact.ReserveRowCapacityOneTime(size, in_memory_row_capacity_vector);
	cout<<">>>>start<<<<"<<endl;
	File *fp = File::OpenOrDie("./fact_table_data","r");
	FailureOrOwned<Cursor> cu(FileInput(fact_schema, fp, false,
			HeapBufferAllocator::Get()));
	vector<StorageType>  fact_storage({DISK,DISK,DISK});
	vector<StorageType>  dim1_storage({MEMORY});
	vector<StorageType>  dim2_storage({MEMORY});
	while(1)
	{
		ResultView result(cu->Next(rowGroupSize));
		if(result.has_data())
		{
			const View& view = result.view();
			fact.AppendView(view,fact_storage);
		}else {
			break;
		}
	}
	//fact.view().PrintViewColumnPieceInfo();
	cout<<">>>>>end input fact_table_data<<<<<"<<endl;

	Table dim1(dimension1_schema,HeapBufferAllocator::Get());
	Table dim2(dimension2_schema,HeapBufferAllocator::Get());
	//vector<rowcount_t> in_memory_row_capacity_vector;
	in_memory_row_capacity_vector.clear();
	in_memory_row_capacity_vector.push_back(rowGroupSize);
	dim1.ReserveRowCapacityOneTime(rowGroupSize, in_memory_row_capacity_vector);
	dim2.ReserveRowCapacityOneTime(rowGroupSize, in_memory_row_capacity_vector);
	cout<<">>>>start<<<<"<<endl;
	File *fp1 = File::OpenOrDie("./dimension1_table_data","r");
	FailureOrOwned<Cursor> cu1(FileInput(dimension1_schema, fp1, false,
			HeapBufferAllocator::Get()));
	while(1)
	{
		ResultView result(cu1->Next(rowGroupSize));
		if(result.has_data())
		{
			const View& view = result.view();
			dim1.AppendView(view,dim1_storage);
		}else {
			break;
		}
	}
	//dim1.view().PrintViewColumnPieceInfo();
	//fp1->Close();
	cout<<">>>>>end input dimesion1_table_data<<<<<"<<endl;
	cout<<">>>>start<<<<"<<endl;
	File *fp2 = File::OpenOrDie("./dimension2_table_data","r");
	FailureOrOwned<Cursor> cu2(FileInput(dimension2_schema, fp2, false,
			HeapBufferAllocator::Get()));
	while(1)
	{
		ResultView result(cu2->Next(rowGroupSize));
		if(result.has_data())
		{
			const View& view = result.view();
			dim2.AppendView(view,dim2_storage);
		}else {
			break;
		}
	}
	//dim2.view().PrintViewColumnPieceInfo();
	//fp2->Close();
	cout<<">>>>>end input dimesion2_table_data<<<<<"<<endl;

	for(int i=0;i<1024;i+=10) {
		dim1.SetNull(0,i);
		dim2.SetNull(0,i+1);
	}
	//dim1.SetNull(0, 10);
	//dim1.SetNull(0, 20);
	//dim2.SetNull(0, 3*1024 + 10);
	//dim2.SetNull(0, 7*1024 + 10);

	AggregationSpecification *ag_spe = new AggregationSpecification();
	ag_spe->AddAggregation(SUM,"agg","sum_agg");

	CompoundSingleSourceProjector *ag_gr_p = new CompoundSingleSourceProjector();
	ag_gr_p->add(ProjectNamedAttribute("key_1"));
	ag_gr_p->add(ProjectNamedAttribute("key_2"));

	vector<vector<int>>* filter_order = new vector<vector<int>>({
		vector<int>({0,1}),vector<int>({1,0}),
		vector<int>({0,1}),vector<int>({1,0}),
		vector<int>({0,1}),vector<int>({1,0}),
		vector<int>({0,1}),vector<int>({1,0}),
		vector<int>({0,1}),vector<int>({1,0}),
		vector<int>({0,1})});
	vector<Operation*> lhs_children;
	lhs_children.push_back(ScanView(dim1.view()));
	lhs_children.push_back(ScanView(dim2.view()));

	scoped_ptr<Operation> ag(MeasureAggregate(ag_gr_p,ag_spe,NULL,filter_order,lhs_children,ScanView(fact.view())));
	scoped_ptr<Cursor> cr(SucceedOrDie(ag->CreateCursor()));

	ResultView resv(ResultView(cr->Next(-1)));
	View rv(resv.view());

	cout<<rv.row_count()<<" "<<rv.schema().GetHumanReadableSpecification()<<endl;
	for(int i=0;i<rv.row_count();i++) {
		cout<<rv.column(0).data_plus_offset(i).as<DOUBLE>()[0]<<"||||"
				<<rv.column(1).data_plus_offset(i).as<DOUBLE>()[0]<<"||||"
				<<rv.column(2).data_plus_offset(i).as<INT64>()[0]<<endl;
	}

	return 0;
}



