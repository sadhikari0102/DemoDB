#include "HeapDBFile.h"
#include <fstream>
/*
HeapDBFile::HeapDBFile() {}

HeapDBFile::~HeapDBFile() {

}*/

void HeapDBFile::Add (Record &addme) {
	// returns the length of file in pages
	off_t len = file.GetLength();

	//get the last page
	if(mode=='r' && len>0)
		file.GetPage(&buffer, len-2);
	Record temp;

	if(file_type==heap) {
		mode='w';
		temp.Copy(&addme);
		//try adding the record to the page
		if (buffer.Append(&addme) != 1) {

			if(len>0)
				file.AddPage(&buffer,len-1);
			else
				file.AddPage(&buffer, 0);
			buffer.EmptyItOut();
			buffer.Append(&temp);
		}

	}
}

void HeapDBFile::MoveFirst() {

	if(mode=='w') {
		int len=file.GetLength();
		if(len>0)
			file.AddPage(&buffer,len-1);
		else
			file.AddPage(&buffer, 0);
		buffer.EmptyItOut();
		mode='r';
	}
	GenericDBFile::MoveFirst();
}

int HeapDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

	while (1) {
		//if the current buffer has no more record, fetch next page from the file
		if(buffer.GetFirst(&fetchme) == 0 ) {
			buffer.EmptyItOut();
			page_index++;
			if (page_index==file.GetLength()-1)
				return 0;

			file.GetPage (&buffer,page_index);
			buffer.GetFirst(&fetchme);
		}

		ComparisonEngine compare;

		//return the record only when it matches with the given literal
		if (compare.Compare(&fetchme, &literal, &cnf) == 1)
			return 1;
	}
}

int HeapDBFile::Close() {
	if(mode=='w') {
		int len=file.GetLength();
		if(len>0)
			file.AddPage(&buffer,len-1);
		else
			file.AddPage(&buffer, 0);
		buffer.EmptyItOut();
		mode='r';
	}

    //creating and  writing metadata file with filetype and related data

    string fname = filepath+filename+".txt";
    ofstream metafile(fname.c_str(),ios_base::out);
    metafile<<file_type<<"\n";
    metafile.close();

	return GenericDBFile::Close();
}

