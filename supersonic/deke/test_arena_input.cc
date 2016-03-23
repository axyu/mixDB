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
	Table table(schema,HeapBufferAllocator::Get());

	while(1)
	{
		ResultView result(cu->Next(1000));
		if(result.has_data())
		{
			table.AppendView(result.view());
			cout<<i<<" "<<result.view().row_count()<<endl;
			i+=result.view().row_count();
		}else
		{
			break;
		}
	}
	cout<<i<<endl;
	cout<<"table row_count = "<<table.view().row_count()<<endl;
	/*
	cu->Next(8190);
	ResultView result(cu->Next(1024));
	View resultView(result.view());
	cout<<">>>>end<<<<"<<endl;
	const int32* key = resultView.column(0).typed_data<INT32>();
	const double* val = resultView.column(1).typed_data<DOUBLE>();

	for(int i=0;i<6;i++)
	{
		cout<<key[i]<<" "<<val[i]<<endl;
	}
	cout<<resultView.schema().GetHumanReadableSpecification()<<endl;
	cout<<resultView.row_count()<<endl;
	*/
	return 0;
}
