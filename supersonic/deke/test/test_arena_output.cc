#include <iostream>

#include "supersonic/supersonic.h"

#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/utils/basictypes.h"

#include <sstream>
using namespace std;

using namespace supersonic;

int main()
{

	const unsigned size = 1024* 10 + 200;
	int *a=(int *)malloc(size*sizeof(int));
	double *b=(double *)malloc(size*sizeof(double));
	StringPiece *str=(StringPiece *)malloc(size*sizeof(StringPiece));
	Arena arena(32,128);
	string tt="test";
	for(int i=0;i<size;i++)
	{
		a[i]=i;
		b[i]=i*0.1;
		stringstream ss;
		ss << tt << i;
		string tmp = ss.str();
		str[i]=StringPiece(arena.AddStringPieceContent(tmp),tmp.length());
	}

	TupleSchema schema;

	schema.add_attribute(Attribute("str",STRING,NOT_NULLABLE));
	schema.add_attribute(Attribute("a",INT32,NOT_NULLABLE));
	schema.add_attribute(Attribute("b",DOUBLE,NOT_NULLABLE));

	View input_view(schema);

	input_view.set_row_count(size);
	input_view.mutable_column(0)->Reset(str,NULL);
	input_view.mutable_column(1)->Reset(a,NULL);
	input_view.mutable_column(2)->Reset(b,NULL);

	cout<<">>>>start<<<<"<<endl;
	File* fp=File::OpenOrDie("./ab","w");
	scoped_ptr<Sink> sink(FileOutput(fp, TAKE_OWNERSHIP));
	cout<<sink->Write(input_view).is_failure()<<endl;
	sink->Finalize();
	cout<<">>>>end<<<<"<<endl;
	free(a);
	free(b);
	//fp->Close();
	return 0;
}
