#include "supersonic/base/infrastructure/block_util.h"
#include <iostream>
using std::cout;
using std::endl;
using namespace supersonic;

int main() {
	cout << SharedSaveType::get() <<endl ;
	SharedSaveType::set(INDISK) ;
	cout << SharedSaveType::get() <<endl ;
}
