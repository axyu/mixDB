/*
 * test_viewIterator.cc
 *
 *  Created on: Mar 15, 2016
 *      Author: fish
 */


#include <iostream>
#include "supersonic/supersonic.h"

using namespace std;
using namespace supersonic;
int main() {
	const unsigned size = 1024;
	int *a=(int *)malloc(size*sizeof(int));
	double *b=(double *)malloc(size*sizeof(double));
	for(int i=0;i<size;i++)
	{
		a[i]=i;
		b[i]=i*0.1;
	}

	TupleSchema schema;
	schema.add_attribute(Attribute("a",INT32,NOT_NULLABLE));
	schema.add_attribute(Attribute("b",DOUBLE,NOT_NULLABLE));

	View input_view(schema);

	input_view.set_row_count(size);
	input_view.mutable_column(0)->Reset(a,NULL);
	input_view.mutable_column(1)->Reset(b,NULL);

	ViewIterator iterator(input_view);
	int i=0;
	while(iterator.next(10)) {
		cout<<"<<<<<<<<<"<<i++<<endl;
	}

	return 0;
}

