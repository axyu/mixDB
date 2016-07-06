#include <iostream>

#include "supersonic/supersonic.h"

#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/utils/basictypes.h"


using namespace std;

using namespace supersonic;

int main()
{
	TupleSchema schema;
	schema.add_attribute(Attribute("str",STRING,NOT_NULLABLE));
	schema.add_attribute(Attribute("a",INT32,NOT_NULLABLE));
	schema.add_attribute(Attribute("b",DOUBLE,NOT_NULLABLE));
	cout<<">>>>start<<<<"<<endl;
	File *fp = File::OpenOrDie("./ab","r");
	FailureOrOwned<Cursor> cu(FileInput(schema, fp, false,
			HeapBufferAllocator::Get()));
	int64 i=0;
	rowcount_t total_row_capacity = 1024*10 + 200;
	vector<rowcount_t> in_memory_row_capacity_vector;
	in_memory_row_capacity_vector.push_back(0);
	in_memory_row_capacity_vector.push_back(0);
	in_memory_row_capacity_vector.push_back(0);
	Table table(schema,HeapBufferAllocator::Get());
	table.ReserveRowCapacityOneTime(total_row_capacity,in_memory_row_capacity_vector);

	vector<StorageType> spt({DISK, DISK, DISK});

	while(1)
	{
		ResultView result(cu->Next(1024));
		if(result.has_data())
		{
			table.AppendView(result.view(),spt);
			//cout<<i<<" "<<result.view().row_count()<<endl;
			i+=result.view().row_count();
		}else
		{
			break;
		}
	}
	cout<<"table row_count = "<<table.view().row_count()<<endl;
	const View& view = table.view();
	const vector<shared_ptr<ColumnPiece>>& cpstr = view.column(0).column_piece_vector();
	const vector<shared_ptr<ColumnPiece>>& cpa = view.column(1).column_piece_vector();
	const vector<shared_ptr<ColumnPiece>>& cpb = view.column(2).column_piece_vector();
	//const vector<shared_ptr<ColumnPiece>>& cpb = view.column(1).column_piece_vector();
	cout<<cpa.size()<<">>>>>>>>>>>>>"<<cpb.size()<<"  ||"<<endl;
	for(int i=0; i<cpa.size(); i++){
		PrintColumnPiece<STRING>(*cpstr[i]);
		PrintColumnPiece<INT32>(*cpa[i]);
		PrintColumnPiece<DOUBLE>(*cpb[i]);
	}
	//view.PrintViewColumnPieceInfo();

	/*
		cout<<"table input piece info "<<cpa[i]->offset()<<" "<<cpa[i]->size()<<" | "<<cpb[i]->offset()<<" "<<cpb[i]->size()<<endl;
		int end = 1024;
		if(i == cpa.size() - 1) end = 200;
		for(int j = 0; j < end; j++) {
		cout<<"table input piece data "
			<<view.column(0).data_plus_offset(i*1024+j).as<DOUBLE>()[0]<<" "
			<<view.column(1).data_plus_offset(i*1024+j).as<INT32>()[0]<<endl;
		cout<<"j: "<<j<<endl;
		}
		}*/
	cout<<">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"<<endl;
	cout<<view.row_count()<<endl;
	return 0;
}
