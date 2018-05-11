#include "SortedDBFile.h"
#include <cstdlib>
#include <cstdio>
#include <fstream>
/*
SortedDBFile::SortedDBFile() {

}

SortedDBFile::~SortedDBFile() {

}*/

int SortedDBFile::Create(const char *f_path, fType f_type, void *startup) {

	typedef struct {OrderMaker *o; int l;}sortInfo;
	sortInfo *start = (sortInfo*)startup;
	ordermaker = *(start->o);
	runlen = start->l;
	return GenericDBFile::Create(f_path, f_type, startup);

}


int SortedDBFile::Open (const char *f_path, string fname) {
    // if file type is not heap, reading order maker
    ifstream metafile(fname.c_str());
    int type;
	metafile>>type>>ordermaker>>runlen;
	/*if(!metafile->eof()) {
    	//metafile->read((char*)&ordermaker, sizeof(ordermaker));
    	metafile>>
    	//cout<<"\nOrder maker loaded!";
    }
	else {
		cout<<"\nFile type: SORTED. Couldn't find the order maker in metadata, aborting!!\n";
		metafile->close();
		return 0;
	}

    // if file type is not heap, reading run length
    if(!metafile->eof()) {
    	string len;
    	getline(*metafile,len);
    	runlen = atoi(len.c_str());
    	//cout<<"\nRunlength - "<<runlen;
    }
	else {
		cout<<"\nFile type: SORTED. Couldn't find run length in metadata, aborting!!\n";
		metafile->close();
		return 0;
	}*/

	metafile.close();
	return GenericDBFile::Open(f_path, fname);
}

void SortedDBFile::Add (Record &addme) {
	if(mode!='w') {
		//a new queue needs to be open
		//create new queue and change the mode to WRITE
		mode = 'w';
		input = new Pipe(PIPE_SIZE);
		output = new Pipe(PIPE_SIZE);
		bigQ = new BigQ(*input, *output, ordermaker, runlen);

	}

	//insert the record in the input pipe for queue to consume
	input->Insert(&addme);
}

void SortedDBFile::MoveFirst() {
	//if sorted file is open in WRITE mode, merge the data first, which will change the mode to READ
	if(mode=='w')
		MergeData();
	GenericDBFile::MoveFirst();
}

int SortedDBFile::GetNext (Record &fetchme) {
	if(mode=='w') {
		MergeData();
	}
	return GenericDBFile::GetNext(fetchme);
}

int SortedDBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	if(mode=='w') {
		MergeData();
	}
	OrderMaker query, cnforder;
	ComparisonEngine compare;

	//Generate a single order maker intersecting both sort order and cnf order
	CustomOrderMaker(query, ordermaker, cnforder, cnf);

	if(!GetNext(fetchme))
		return 0;

	//start with the first record or the current record and binary search the file for optimized position
	else {
		BinarySearch(fetchme, literal, compare, query, cnforder);
	}

	while(true) {

		int var = compare.Compare(&fetchme, &query, &literal, &cnforder);
		//ensure the correctness of record based on provided literal
		if(var!=0)
			return 0;

		//ensure the fetched record fit the cnf form
		var = compare.Compare(&fetchme, &literal, &cnf);
		if(var!=0)
			return 1;
		if(!GetNext(fetchme))
			return 0;
	}
}

int SortedDBFile::Close() {
	//creating and  writing metadata file with filetype and related data
	string fname = filepath+filename+".txt";
	ofstream metafile(fname.c_str());

	//metafile.open(fname.c_str(),ios_base::out);
	//metafile<<file_type<<"\n";

	//writing ordermaker object and run length into metafile

	//metafile.write((char*)&ordermaker, sizeof(ordermaker));
	metafile<<"1\n"<<ordermaker<<"\n"<<runlen<<endl;

	metafile.close();

	//if user tries to close it in WRITE mode, merge the data first from the pipe to the file
	if(mode=='w')
		MergeData();
	return GenericDBFile::Close();
}



/*It is called when WRITE mode is on and user is trying to read from it. In such cases, the mode is changed to READ,
 and all the records in the pipe are merged with the existing records in the DBFile in a sorted order*/
void SortedDBFile::MergeData () {
	//change the mode to READ and start merging the pipe with the existing file
	cout<<"\nmerging data!!!\n";
	mode = 'r';
	input->ShutDown();
	int count=0;
	File *tempoutput = new File();
	Page *localbuffer = new Page();
	tempoutput->Open(0,(char*)((filepath+"tempoutput.bin").c_str()));
	MoveFirst();
	Record record1, record2;

	int checkFile = GetNext(record1);

	int checkBigQ = output->Remove(&record2);

	ComparisonEngine compare ;
	Record rec;
	Record temp;
	//when both file and pipe contain records, compare and merge
	while(checkFile==1 && checkBigQ==1) {

		int c = compare.Compare(&record1,&record2,&ordermaker);
		if(c<=0) {

			rec.Copy(&record1);
			temp.Copy(&record1);
			checkFile = GetNext(record1);
		}
		else {

			rec.Copy(&record2);
			temp.Copy(&record2);
			checkBigQ = output->Remove(&record2);
		}

		//if page is full, add the page to file and empty the page and start filling again
		if(localbuffer->Append(&temp)==0) {
			int which_page=0;
			int len = tempoutput->GetLength();
			if(len>0)
				which_page = len-1;
			else
				which_page = 0;

			tempoutput->AddPage(localbuffer, which_page);
			localbuffer->EmptyItOut();
			localbuffer->Append(&rec);
		}
		count++;
	}
	//if some records in the file still remain
	while(checkFile==1) {
		rec.Copy(&record1);
		temp.Copy(&record1);

		//if page is full, add the page to file and empty the page and start filling again
		if(localbuffer->Append(&temp)==0) {
			int which_page=0;
			int len = tempoutput->GetLength();
			if(len>0)
				which_page = len-1;
			else
				which_page = 0;

			tempoutput->AddPage(localbuffer, which_page);
			localbuffer->EmptyItOut();
			localbuffer->Append(&rec);
		}
		count++;
		checkFile = GetNext(record1);
	}
	//if some record in the pipe still remain
	while(checkBigQ==1) {
		rec.Copy(&record2);
		temp.Copy(&record2);

		//if page is full, add the page to file and empty the page and start filling again
		if(localbuffer->Append(&temp)==0) {
			int which_page=0;
			int len = tempoutput->GetLength();
			if(len>0)
				which_page = len-1;
			else
				which_page = 0;

			tempoutput->AddPage(localbuffer, which_page);
			localbuffer->EmptyItOut();
			localbuffer->Append(&rec);
		}
		count++;
		checkBigQ = output->Remove(&record2);
	}
	//inserting the last partially filled page into the file
	int which_page=0;
	int len = tempoutput->GetLength();
	if(len>0)
		which_page = len-1;
	else
		which_page = 0;
	tempoutput->AddPage(localbuffer, which_page);
	tempoutput->Close();
	rename((filepath+"tempoutput.bin").c_str(), fullname.c_str());
	cout<<"\n"<<count<<" records merged into file\n";
}

/*Binary search on the file to find a prospective page with literal record match,
Once the page is found, iterate on it to find the very first record matching with the literal*/
int SortedDBFile::BinarySearch(Record& fetchme, Record& literal, ComparisonEngine& compare, OrderMaker& query, OrderMaker& cnforder) {
	int var = compare.Compare(&fetchme, &query, &literal, &cnforder);
	//current record matched with the literal
	if(var==0)
		return 1;
	//literal record beyond limit
	else if(var>0)
		return 0;

	//binary search for the page in which the record is to be expected
	else {
		int start=page_index, end = file.GetLength()-2;
		int mid = (start+end)/2;
		while(start<mid) {
			file.GetPage(&buffer, mid);
			if(!GetNext(fetchme))
				return 0;
			var = compare.Compare(&fetchme, &query, &literal, &cnforder);
			//decide which half to traverse
			if(var==0)
				end=mid;
			else if(var>0)
				end=mid-1;
			else
				start=mid;

		}
		file.GetPage(&buffer, start);

		//traverse the prospective page to find a appropriate starting position
		while(true) {
			if(!GetNext(fetchme))
				return 0;
			else
				var=compare.Compare(&fetchme, &query, &literal, &cnforder);
			if(var==0)
				return 1;
			else if(var>0)
				return 0;
		}
	}
}

/*Custom order maker which takes original sort order of the file and order of arguments in provided CNF
and creates a new order maker as an intersection of both the inputs*/
void SortedDBFile::CustomOrderMaker(OrderMaker &final, OrderMaker &sort, OrderMaker &cnforder, CNF &cnf) {

	int count =0;
	// keep running the loop on until common attributes are being discovered between sort ordermaker and cnf ordermaker, then break
	for(int i=0;i<sort.numAtts;i++) {
		int attr = sort.whichAtts[i];
		Type atype = sort.whichTypes[i];
		int index=-1;
		// find this attribute in supplied CNF
		for(int j=0;j<cnf.numAnds;j++) {
			if(cnf.orLens[j]==1) {
				Comparison compare = cnf.orList[j][0];
				if(compare.op==Equals && (((compare.whichAtt1==attr) && (compare.operand2==Literal))
			              ||((compare.whichAtt2==attr) && (compare.operand1==Literal))))
					index=j;
			}
		}
		//attribute common
		if(index>=0) {
			final.whichAtts[count]=attr;
			final.whichTypes[count]=atype;
			cnforder.whichAtts[count]=index;
			cnforder.whichTypes[count]=atype;
			final.numAtts=count;
			cnforder.numAtts=count;
			count++;
		}
		//attribute not found in cnf
		else
			break;


	}
}



