#include <iostream>

#include "supersonic/supersonic.h"

using namespace std;

using namespace supersonic;

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

	const Expression *fe = LessOrEqual(NamedAttribute("a"),ConstInt32(20));
	CompoundSingleSourceProjector *fp= new CompoundSingleSourceProjector();
	fp->add(ProjectNamedAttribute("a"));
	//fp->add(ProjectNamedAttribute("b"));

	scoped_ptr<Operation> filter(Filter(fe,fp,ScanView(input_view)));
	scoped_ptr<Cursor> filter_cursor(SucceedOrDie(filter->CreateCursor()));
/*
	scoped_ptr<Operation> sco(ScanViewWithSelection(input_view,1024,sv,512));

	scoped_ptr<Cursor> scan_cursor(SucceedOrDie(sco->CreateCursor()));

	ResultView result(scan_cursor->Next(3));

	View resultView(result.view());
*/
	ResultView result(filter_cursor->Next(-1));
	View resultView(result.view());

	cout<<resultView.schema().GetHumanReadableSpecification()<<endl;
	cout<<resultView.row_count()<<endl;
	/*
	const int32* key = resultView.column(0).typed_data<INT32>();
	const double* val = resultView.column(1).typed_data<DOUBLE>();

	for(int i=0;i<6;i++)
	{
		cout<<key[i]<<" "<<val[i]<<endl;
	}
	*/
	return 0;
}
