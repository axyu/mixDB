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
		b[i]=i*1.0;
		sv[i/2]=i;
	}
	TupleSchema schema;
	schema.add_attribute(Attribute("a",INT32,NOT_NULLABLE));
	schema.add_attribute(Attribute("b",DOUBLE,NOT_NULLABLE));

	View input_view(schema);

	input_view.set_row_count(size);
	input_view.mutable_column(0)->Reset(a,NULL);
	input_view.mutable_column(1)->Reset(b,NULL);

	scoped_ptr<const Expression> addition(Plus(AttributeAt(0),AttributeAt(1)));
	FailureOrOwned<BoundExpressionTree> bound_addition = addition->Bind(schema,HeapBufferAllocator::Get(),2048);

	EvaluationResult result = bound_addition->Evaluate(input_view);
	/*
	const Expression *fe = LessOrEqual(NamedAttribute("a"),ConstInt32(20));
	CompoundSingleSourceProjector *fp= new CompoundSingleSourceProjector();
	fp->add(ProjectNamedAttribute("a"));

	scoped_ptr<Operation> filter(Filter(fe,fp,ScanView(input_view)));
	scoped_ptr<Cursor> filter_cursor(SucceedOrDie(filter->CreateCursor()));

	ResultView result(filter_cursor->Next(-1));
	*/
	View resultView(result.get());

	const double *pl = resultView.column(0).typed_data<DOUBLE>();

	for(int i=0;i<resultView.row_count();i++)
	{
		cout<<pl[i]<<endl;
	}

	cout<<resultView.schema().GetHumanReadableSpecification()<<endl;
	cout<<resultView.row_count()<<endl;

	cout<<a<<" "<<b<<" "<<pl<<endl;
	return 0;
}
