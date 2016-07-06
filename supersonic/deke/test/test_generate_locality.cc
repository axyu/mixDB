#include <iostream>
#include "supersonic/supersonic.h"
#include <vector>
#include "deke/include/generate_test_locality.h"

using std::cout;
using std::endl;
using std::vector;

using namespace supersonic;

int main() {
	rowcount_t row_group_count = 100;
	vector<rowcount_t> range({0,20,40,60,80,100});
	vector<double> probality({0.1,0.1,0.1,0.1,0.6});
	vector<double> result = generate_test_locality(row_group_count,range,probality);
	cout<<result.size()<<endl;
	cout<<"//////////"<<endl;
	for(int i = 0; i < result.size(); i++) {
		cout<<result[i]<<endl;
	}
	cout<<"<<<<<"<<endl;
	double sum = 0.0,total_sum = 0.0;
	int i;
	for(i = 0; i < result.size(); i++) {
		if( i % 20 == 0 ) {
			cout<<i<<" >> "<<sum<<endl;
			sum = result[i];
		}
		sum += result[i];
		total_sum += result[i];
	}
	cout<<i<<" >> "<<sum<<endl;
	cout<<" total_sum "<<total_sum<<endl;
	return 0;
}

