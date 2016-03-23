#include <iostream>
#include "supersonic/supersonic.h"

#include "supersonic/utils/file.h"
#include "supersonic/cursor/infrastructure/file_io.h"
#include "supersonic/utils/basictypes.h"


using namespace std;

using namespace supersonic;

int main()
{
	File *fp = File::OpenOrDie("./test_F","w+");
	char s[10];
	for(int i=0;i<10;i++)
	{
		sprintf(s,"%f",i*3.1415);
		string str(s);
		fp->Write(str.c_str(),str.length());
	}
	//fp->Close();

	//fp = File::OpenOrDie("./test_F","r");
	fp->Seek(0);
	fp->Read(s,3);
	s[3]='\0';
	cout<<s<<endl;
	fp->Read(s,2);
	s[2]='\0';
	cout<<s<<endl;
	fp->Seek(4);
	fp->Read(s,2);
	s[2]='\0';
	cout<<s<<endl;
	cout<<"file size = "<<fp->FileSize()<<endl;
	fp->Close();
	return 0;
}
