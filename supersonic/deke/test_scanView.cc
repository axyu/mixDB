#include <iostream>

#include "supersonic/supersonic.h"

using std::cout;

using std::endl;

using supersonic::BoundExpressionTree;

using supersonic::Expression;

using supersonic::Plus;

using supersonic::AttributeAt;

using supersonic::TupleSchema;

using supersonic::Attribute;

using supersonic::INT32;

using supersonic::DOUBLE;

using supersonic::NOT_NULLABLE;

using supersonic::FailureOrOwned;

using supersonic::HeapBufferAllocator;

using supersonic::View;

using supersonic::EvaluationResult;

using supersonic::SingleSourceProjector;

using supersonic::Operation;

using supersonic::Cursor;

using supersonic::ResultView;


int main()
{
	const unsigned size = 2048;
	int a[size];
	double b[size];
	int64 sv[size/2];
	for(int i=0;i<size;i++)
	{
		a[i]=i;
		b[i]=i*0.1;
		sv[i/2]=i;
	}
	TupleSchema schema;
	schema.add_attribute(Attribute("a",INT32,NOT_NULLABLE));
	schema.add_attribute(Attribute("b",DOUBLE,NOT_NULLABLE));

	View input_view(schema);

	input_view.set_row_count(size);
	input_view.mutable_column(0)->Reset(a,NULL);
	input_view.mutable_column(1)->Reset(b,NULL);

	//scoped_ptr<Operation> sco(ScanViewWithSelection(input_view,1024,sv,512));

	scoped_ptr<Operation> sco(ScanView(input_view));

	scoped_ptr<Cursor> scan_cursor(SucceedOrDie(sco->CreateCursor()));

	ResultView result(scan_cursor->Next(3));

	View resultView(result.view());

	const int32* key = resultView.column(0).typed_data<INT32>();
	const double* val = resultView.column(1).typed_data<DOUBLE>();

	for(int i=0;i<6;i++)
	{
		cout<<key[i]<<" "<<val[i]<<endl;
	}

	return 0;
}
