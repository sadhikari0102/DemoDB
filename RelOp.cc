#include "RelOp.h"

int RelationalOp::Launch_Thread(void*(*thread_func) (void*), void* arg) {
	//setting up attributes to make thread joinable
	pthread_attr_t attributes;
	pthread_attr_init(&attributes);
	pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_JOINABLE);

	//launching thread
	int flag = pthread_create(&operation, &attributes, thread_func, arg);
	pthread_attr_destroy(&attributes);

	//check for thread errors while launching
	return flag;
}

void RelationalOp::WaitUntilDone () {
	pthread_join(operation, NULL);
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	input ={&inFile, &outPipe, &selOp, &literal};

	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}


void* SelectFile::Thread_Function(void* param) {

	//cout<<"Inside Thread SelectFile";
	SelectFile* obj = (SelectFile*)param;

	int count=0;
	opinput *input = &(obj->input);
	Record temp;
	DBFile* file = input->dbfile;
	CNF* c=input->cnf;
	Record* lit = input->record;
	Pipe* out = input->pipe;
	file->MoveFirst();
	//cout<<"\nmoved to first";
	while(file->GetNext(temp, *c, *lit)) {
		//temp.Print(new Schema("catalog","partsupp"));
		out->Insert(&temp);
		count++;
	}
	out->ShutDown();
	cout<<"\n"<<count<<"rows returned from SelectFile\n";

}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	input ={&inPipe, &outPipe, &selOp, &literal};
	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}


void* SelectPipe::Thread_Function(void* param) {

	SelectPipe* obj = (SelectPipe*)param;


	opinput *input = &(obj->input);
	ComparisonEngine compare;
	Record temp;
	Pipe* inpipe = input->inpipe;
	CNF c=*(input->cnf);
	Record lit = *(input->record);
	Pipe* outpipe = input->outpipe;
	int count=0;
	while(inpipe->Remove(&temp)) {
		if(compare.Compare(&temp, &lit, &c)) {
			outpipe->Insert(&temp);
			count++;
		}
	}
	outpipe->ShutDown();

	cout<<"\n"<<count<<"rows returned from SelectiPipe\n";

}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	input ={&inPipe, &outPipe, keepMe, numAttsInput, numAttsOutput};
	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}


void* Project::Thread_Function(void* param) {

	Project* obj = (Project*)param;

	int count=0;
	opinput *input = &(obj->input);
	Record temp;
	Pipe* inpipe = input->inpipe;
	Pipe* outpipe = input->outpipe;
	const int* toKeep = input->keepMe;
	while(inpipe->Remove(&temp)) {
		temp.Project(toKeep, input->numAttsOutput, input->numAttsInput);
		//cout<<count<<",";
		//temp.Print(new Schema("catalog","partsupp"));
		outpipe->Insert(&temp);
		count++;
	}
	outpipe->ShutDown();

	cout<<"\n"<<count<<"rows returned from Project\n";

}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	input ={&inPipe, &outPipe, &computeMe};
	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}


void* Sum::Thread_Function(void* param) {
	Sum* obj = (Sum*)param;


	opinput *input = &(obj->input);
	Record temp;
	Pipe* inpipe = input->inpipe;
	Pipe* outpipe = input->outpipe;
	Function* computeMe = input->computeMe;
	int count=0;
	if(computeMe->returnsInt)
		count=Add<int>(inpipe, outpipe, computeMe);
	else
		count=Add<double>(inpipe, outpipe, computeMe);

	outpipe->ShutDown();

	//cout<<"\n"<<count<<"result returned from SUM\n";
}

template <class T>
int Sum::Add(Pipe* in, Pipe* out, Function* compute) {
	T sum=0;
	Record temp;
	int x;
	double y;
	while (in->Remove(&temp)) {
		compute->Apply(temp, x, y);
		if(compute->returnsInt)
			sum += x;
		else
			sum+= y;
	}
	Record result(sum);
	out->Insert(&result);
	return sum;
}

Join::Join() {
	runlength=100;
}

Join::~Join() {

}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	input ={&inPipeL, &inPipeR, &outPipe, &selOp, &literal, runlength};
	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}

void Join::Use_n_Pages (int n) {
	runlength = n;
}

void* Join::Thread_Function(void* param) {

	Join* obj = (Join*)param;
	int count=0;

	opinput *input = &(obj->input);
	OrderMaker left, right;
	int flag = input->selOp->GetSortOrders(left, right);
	if(flag)
		count=SortAndMerge(input->leftpipe, input->rightpipe, input->outpipe, &left, &right, input->selOp, input->literal, input->runlength);
	else
		count=BlockNestedLoop(input->leftpipe, input->rightpipe, input->outpipe, input->selOp, input->literal, input->runlength);
	input->outpipe->ShutDown();

	cout<<"\n"<<count<<"rows returned from Join\n";

}

int Join::SortAndMerge(Pipe* leftpipe, Pipe* rightpipe, Pipe* outpipe, OrderMaker* left, OrderMaker* right, CNF* selOp, Record* literal, size_t runlength) {
	//int pipesize = 100;
	ComparisonEngine compare;
	cout<<"\nSort-Merge Join!!!\n";
	Pipe sortedLeft(PIPE_SIZE), sortedRight(PIPE_SIZE);

	//queues representing individually sorted two input pipes to be joined
	BigQ qLeft(*leftpipe, sortedLeft, *left, runlength);
	BigQ qRight(*rightpipe, sortedRight, *right, runlength);

	Record fromLeft, fromRight, previous;
	Record merged; //this will hold individual merged record
	//cout<<"\nOM L "<<left->whichAtts[0];
	//cout<<"\nOM R "<<right->whichAtts[0];
	int count=0;

	//A custom structure capable of holding records in the form of an array
	RecordBuffer buffer(runlength);
	int leftr=0, rightr=0;
	//fetch and merge until any one of the pipes is empty
	for (int l1 = sortedLeft.Remove(&fromLeft), r1 = sortedRight.Remove(&fromRight);  l1 && r1; ) {
		//compare the join attributes for equivalence
		int flag = compare.Compare(&fromLeft, left, &fromRight, right);
		//inloop++;

		//left is less than right, so fetch next left
		if (flag<0) {
			//fromLeft.Print(new Schema("catalog","supplier"));
			//cout<<"\n";
			//fromRight.Print(new Schema("catalog","partsupp"));
			l1 = sortedLeft.Remove(&fromLeft);
			leftr++;
		}
		//right is less than left, so fetch next right
		else if (flag>0) {
			//fromRight.Print(new Schema("catalog","partsupp"));
			r1 = sortedRight.Remove(&fromRight);
			rightr++;
		}
		// proceed for the join only when left is equal to right
		else {
			buffer.Refresh();
			previous.Consume(&fromLeft);
			//inhere++;
			//collect all equal values records from left into buffer

			while((l1=sortedLeft.Remove(&fromLeft)) && compare.Compare(&previous, &fromLeft, left)==0) {
				if(!buffer.Insert(previous))
					cout<<"\nBuffer Overflow!!\n";
				previous.Consume(&fromLeft);
			}
			//cout<<"here";
			if(!buffer.Insert(previous))
				cout<<"\nBuffer Overflow!!\n";

			// for every record in left buffer, join with every equal record in right pipe
			do {

				for (Record* p=buffer.data; p!=buffer.data+(buffer.count); ++p) {
					Record &rec = *p;
					if (compare.Compare(&rec, &fromRight, literal, selOp)) {
						// merge left and right
						MergeRecords(&rec, &fromRight, &merged);
						outpipe->Insert(&merged);
						//cout<<"\nin";
						count++;
					}
				}
			}
			//continue till right still has values and, left and right are equal
			while ((r1=sortedRight.Remove(&fromRight)) && compare.Compare(buffer.data, left, &fromRight, right)==0);
		}
	}
	//cout<<"\nleft: "<<leftr<<" times";
	//cout<<"\nright: "<<rightr<<" times";
	return count;
}

void RelationalOp::MergeRecords(Record* left, Record* right, Record* result) {

	int leftAtt = (left->GetSize()<=4) ? 0 : left->GetPointer(0)/sizeof(int)-1;
	int rightAtt = (right->GetSize()<=4) ? 0 : right->GetPointer(0)/sizeof(int)-1;
	int total = leftAtt + rightAtt;
	int* attsToKeep = new int[total];
	for(int i=0; i<leftAtt; ++i)
		attsToKeep[i] = i;
	for(int i=0; i<rightAtt; ++i)
		attsToKeep[i+leftAtt] = i;
	result->MergeRecords (left, right, leftAtt, rightAtt, attsToKeep, total, leftAtt);
	delete[] attsToKeep;
}

int Join::BlockNestedLoop(Pipe* leftpipe, Pipe* rightpipe, Pipe* outpipe, CNF* selOp, Record* literal, size_t runlength) {
	DBFile tempFile;
	tempFile.Create("TemporaryRecords.txt", heap, NULL);
	//cout<<"\nBlock-Nested Join\n";
	Record rec;
	while (rightpipe->Remove(&rec))
		tempFile.Add(rec);

	RecordBuffer buffer(runlength);
	int count=0;
	// nested loops join
	while(leftpipe->Remove(&rec)) {
		if(!buffer.Insert(rec)) {
			count+=FileAndBuffer(buffer, tempFile, *outpipe, *literal, *selOp);
			buffer.Refresh();
			buffer.Insert(rec);
		}
	}

	count+=FileAndBuffer(buffer, tempFile, *outpipe, *literal, *selOp);
	tempFile.Close();
	return count;
}

int Join::FileAndBuffer(RecordBuffer &buffer, DBFile& file, Pipe& pipe, Record& literal, CNF& selOp) {
	ComparisonEngine compare;
	Record merged;
	Record fromFile, fromBuffer;
	file.MoveFirst();
	int count=0;
	//check and merge for the combination of all records in file and in buffer
	while(file.GetNext(fromFile)) {
		for (Record* p=buffer.data; p!=buffer.data+(buffer.count); ++p) {
			Record &rec = *p;
			//compare the record from file and from buffer for equality
			if (compare.Compare(&rec, &fromFile, &literal, &selOp)) {
				//merge rec and fromFile into merged
				MergeRecords(&rec, &fromFile, &merged);
				pipe.Insert(&merged);
				count++;
			}
		}
	}
	return count;
}

DuplicateRemoval::DuplicateRemoval() {
	runlength = 100;
}

DuplicateRemoval::~DuplicateRemoval() {

}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	input ={&inPipe, &outPipe, &mySchema, runlength};
	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}

void DuplicateRemoval::Use_n_Pages (int n) {
	runlength = n;
}

void* DuplicateRemoval::Thread_Function(void* param) {
	ComparisonEngine compare;

	DuplicateRemoval* obj = (DuplicateRemoval*)param;


	opinput *input = &(obj->input);

	OrderMaker order(input->myschema);

	//Queue for sorting the incoming inputs
	Pipe sortOut(PIPE_SIZE);
	BigQ sort(*(input->inpipe), sortOut, order, (int)input->runlength);

	Record previous;
	int count=0;
	int flag = sortOut.Remove(&previous);
	// if the pipe is not empty
	if(flag) {
		Record current;
		//match current record with the previous one and only insert the previous one into output pipe if it is not equal to current
		while(sortOut.Remove(&current)) {
			if(compare.Compare(&previous, &current, &order)) {
				input->outpipe->Insert(&previous);
				previous.Consume(&current);
				count++;
			}
		}
		input->outpipe->Insert(&previous);
		count++;
	}

	input->outpipe->ShutDown();
	cout<<"\n"<<count<<"rows returned from Duplicate removal\n";
}

GroupBy::GroupBy() {
	runlength=100;
}

GroupBy::~GroupBy() {

}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	input ={&inPipe, &outPipe, &groupAtts, &computeMe, runlength};
	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}

void GroupBy::Use_n_Pages (int n) {
	runlength = n;
}

void* GroupBy::Thread_Function(void* param) {

	GroupBy* obj = (GroupBy*)param;

	int count=0;
	opinput *input = &(obj->input);
	if(input->computeMe->returnsInt)
		count=GroupAndOperate<int>(input->inpipe, input->outpipe, input->order, input->computeMe, input->runlength);
	else
		count=GroupAndOperate<double>(input->inpipe, input->outpipe, input->order, input->computeMe, input->runlength);
	input->outpipe->ShutDown();
	cout<<"\n"<<count<<"rows returned from GroupBy\n";
}


template <class T>
int GroupBy::GroupAndOperate(Pipe* in, Pipe* out, OrderMaker* order, Function* compute, size_t runlength) {
	ComparisonEngine compare;
	//temporary output pipe to be populated by BigQ in sorted order
	Pipe sortOut(100);
	order->Print();
	BigQ sort(*in, sortOut, *order, runlength);
	int count=0;
	Record previous;
	//check if input pipe is empty
	int flag=sortOut.Remove(&previous);
	if(flag) {
		int x;
		double y;
		T sum=(compute->Apply(previous, x, y))==Int?x:y;
		Record current;
		//previous.Print(join_sch);
		//for every record, compare the previous record for equivalence, and group it
		while(sortOut.Remove(&current)) {
			//current.Print(join_sch);
			//if comparison returns 0, keep adding to the group
			if(compare.Compare(&previous, &current, order)==0)
				sum+=((compute->Apply(current, x, y)==Int)?x:y);
			//if not same, close the group and start a new sum
			else {
				count++;
				CloseGroup(previous, sum, *out, *order);
				sum =((compute->Apply(current, x, y)==Int)?x:y);
				previous.Consume(&current);
			}
		}
		//close the final group
		count++;
		//cout<<"\nhere !!\n";
		CloseGroup(previous, sum, *out, *order);
	}
	return count;

}

template <class T>
void GroupBy::CloseGroup(Record &previous, T& sum, Pipe&out, OrderMaker&order) {
	//number of attribute in the record
	int previousN = (previous.GetSize()<=4) ? 0 : previous.GetPointer(0)/sizeof(int)-1;
	//project away all the attributes which are not present in the order
	previous.Project(order.whichAtts, order.numAtts, previousN);
	//add the extra attribute, sum, to the final record
	previous.Populate(sum);
	out.Insert(&previous);
}



void WriteOut::Run (Pipe &inPipe, FILE* outFile, Schema &mySchema) {
	input ={&inPipe, outFile, &mySchema};
	if(Launch_Thread(Thread_Function, (void*)this)!=0)
		cout<<"\n Error encountered while creating Thread!\n";
}

void* WriteOut::Thread_Function(void* param) {

	WriteOut* obj = (WriteOut*)param;

	int count=0;
	opinput *input = &(obj->input);
	Record rec;
	FILE *file = input->outfile;
	Schema *mySchema = input->myschema;
	mySchema->PrintHead(file);
	//cout<<"\nIn writeout\n";
	//pushing to the file, one record at a time
	while(input->inpipe->Remove(&rec)) {
		int n = mySchema->GetNumAtts();
		//getting total number of attribute
		Attribute *atts = mySchema->GetAtts();

		//converting to bits for fetching different attributes
		char* bits = rec.GetBits();

		//cout<<"Write out "<<count;

		//loop on the number of attribute and put on to the characted stream
		for (int i = 0; i < n; i++) {

			int pointer = ((int *) bits)[i + 1];
			//depending upon the type of attribute, corresponding output statement is given
			switch(atts[i].myType) {
			case Int: {int *myInt = (int *) &(bits[pointer]);
						fprintf(file, "%d", *myInt);}
						break;
			case Double: {double *myDouble = (double *) &(bits[pointer]);
						fprintf(file, "%f", *myDouble);}
						break;
			case String: {char *myString = (char *) &(bits[pointer]);
						fprintf(file, "%s", myString);}
						break;
			}
			//using : as a attribute separator
			fprintf(file, "\t");
		}
		//changing line after ever tuple
		fprintf(file, "\n");
		count++;
	}
	//cout<<"\nout of loop";
	if(file!=stdout)
		fclose(file);

	//cout<<"\n"<<count<<"rows produced by the query!\n";

}
