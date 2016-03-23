#include <iostream>

#include "supersonic/supersonic.h"

#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/utils/basictypes.h"


using namespace std;

using namespace supersonic;

int main()
{

	const unsigned size = 1024 * 10 + 200;
	int *a=(int *)malloc(size*sizeof(int));
	double *b=(double *)malloc(size*sizeof(double));
	for(int i=0;i<size;i++)
	{
		a[i]=i;
		b[i]=i*0.1;
	}

	TupleSchema schema;
	schema.add_attribute(Attribute("a",DOUBLE,NOT_NULLABLE));
	schema.add_attribute(Attribute("b",INT32,NOT_NULLABLE));

	View input_view(schema);

	input_view.set_row_count(size);
	input_view.mutable_column(0)->Reset(b,NULL,size);
	input_view.mutable_column(1)->Reset(a,NULL,size);
	const vector<shared_ptr<ColumnPiece>>& cpa = input_view.column(0).column_piece_vector();
	const vector<shared_ptr<ColumnPiece>>& cpb = input_view.column(1).column_piece_vector();
	for(int i=0;i<cpa.size();i++){
		cout<<cpa[i]->offset()<<" "<<cpa[i]->size()<<" | "<<cpb[i]->offset()<<" "<<cpb[i]->size()<<endl;
		cout<<"input piece data "
			<<input_view.column(0).data_plus_offset(i *1024).as<DOUBLE>()[0]<<" "
			<<input_view.column(1).data_plus_offset(i * 1024).as<INT32>()[0]<<endl;
	}

	cout<<">>>>start<<<<"<<endl;
	File* fp=File::OpenOrDie("./ab","w");
	scoped_ptr<Sink> sink(FileOutput(fp, TAKE_OWNERSHIP));
	cout<<sink->Write(input_view).is_failure()<<endl;
	sink->Finalize();
	cout<<">>>>end<<<<"<<endl;
	free(a);
	free(b);

	return 0;
}
