#include <iostream>

#include "supersonic/supersonic.h"

#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/utils/basictypes.h"


using namespace std;

using namespace supersonic;

int main()
{
	const rowcount_t row_group_size = rowGroupSize;
	TupleSchema schema;
	schema.add_attribute(Attribute("a",DOUBLE,NOT_NULLABLE));
	schema.add_attribute(Attribute("b",INT32,NOT_NULLABLE));
	cout<<">>>>start<<<<"<<endl;
	File *fp = File::OpenOrDie("./ab","r");
	FailureOrOwned<Cursor> cu(FileInput(schema, fp, false,
	                                      HeapBufferAllocator::Get()));
	int64 i=0;
	Table table(schema,HeapBufferAllocator::Get());
	rowcount_t total_row_capacity = 1024 * 10 + 200;
	vector<rowcount_t> in_memory_row_capacity_vector;
	in_memory_row_capacity_vector.push_back(1024 * 5);
	in_memory_row_capacity_vector.push_back(1024 * 5 + 200);
	table.ReserveRowCapacityOneTime(total_row_capacity, in_memory_row_capacity_vector);
	vector<StorageType> spv({DISK, MEMORY});
	vector<StorageType> spu({MEMORY, DISK});
	vector<StorageType> spw({MEMORY, MEMORY});
	vector<StorageType> spx({DISK, DISK});
	while(1)
	{
		ResultView result(cu->Next(row_group_size));
		if(result.has_data())
		{
			const View& view = result.view();

			 //cout<<i<<" "<<view.row_count()<<"<<"<<endl;
			const vector<shared_ptr<ColumnPiece>>& cpa = view.column(0).column_piece_vector();
			const vector<shared_ptr<ColumnPiece>>& cpb = view.column(1).column_piece_vector();
			//for(int i=0;i<cpa.size();i++){
				cout<<" input piece info "<<cpa[0]->offset()<<" "<<cpa[0]->size()<<" | "<<cpb[0]->offset()<<" "<<cpb[0]->size()<<endl;
				cout<<"input piece data "
						<<view.column(0).data_plus_offset(0).as<DOUBLE>()[0]<<" "
						<<view.column(1).data_plus_offset(0).as<INT32>()[0]<<endl;
				cout<<"storage type " <<cpa[0]->storage_type()<<" "<<cpb[0]->storage_type()<<endl;
				/*
				const int *a = view.column(0).column_piece(0).typed_data<INT32>();
				const double *b = view.column(1).typed_data<DOUBLE>();
				for(int i=0;i<result.view().row_count();i++) {
					cout<<a[i]<<" "<<b[i]<<endl;
					}
*/

			//}
			//i+=view.row_count();
			std::cout << "i: " << i <<std::endl;
			if((i / 1024) % 2 == 0)
				table.AppendView(view, spv);
			else
				table.AppendView(view, spu);
			i+=result.view().row_count();
		}else
		{
			break;
		}
	}

	scoped_ptr<Operation> sco(ScanView(table.view()));

	scoped_ptr<Cursor> scan_cursor(SucceedOrDie(sco->CreateCursor()));
	while(1){
		ResultView result_view(scan_cursor->Next(rowGroupSize));
		if(!result_view.has_data()){
			break;
		}else {
			result_view.view().PrintViewColumnPieceInfo();
		}
	}
	/*
	const View& view = table.view();
	const vector<shared_ptr<ColumnPiece>>& cpa = view.column(0).column_piece_vector();
	const vector<shared_ptr<ColumnPiece>>& cpb = view.column(1).column_piece_vector();
	cout<<cpa.size()<<">>>>>>>>>>>>>"<<cpb.size()<<"  ||"<<endl;
	for(int i=0; i<cpa.size(); i++){
		PrintColumnPiece<DOUBLE>(*cpa[i]);
		PrintColumnPiece<INT32>(*cpb[i]);
	}
	*/
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
	//cout<<">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"<<endl;
	//cout<<view.row_count()<<endl;
	return 0;
}
