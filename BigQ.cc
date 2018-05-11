#include "BigQ.h"
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>
#include <queue>
#include <time.h>

using namespace std;

void* ThreadFunction(void* arg)
{
    Hardware *info=new Hardware();
    info=(Hardware*)arg;
    Pipe *in=info->in;
    Pipe *out=info->out;
    OrderMaker sortorder=info->order;
    int runlen=info->runLength;
    File file;

    //random number to be generated to create a temporary file with unique name for every run
    int num=rand()%10000;
    //filenamed as random number
	char fileName[10];
	sprintf(fileName,"%d",num);
	file.Open(0,fileName);

	Record record;
	Page myPage;
	Page sortPage;
	int pageCounter=0;
	int runCounter=0;
	vector<Record *> vec;
	vector<int> runL;

	int count=0;
	ComparisonEngine compare;

	while(in->Remove(&record)) {
	    count++;
		Record *cRecord=new Record;
	    cRecord->Copy(&record);
	    //fetch one record at a time from the input pipe and push it into the page and in turn into the vector
	    if(myPage.Append(&record))
	        vec.push_back(cRecord);
	    //initialize a new page once it is full
	    else {
	        ++pageCounter;
	        //if the number of pages is equal to runlength, sort the vector and write the data into file before proceeding with further fetch
	        if(pageCounter==runlen) {
	            stable_sort(vec.begin(),vec.end(),SortRecordComparator(&sortorder));
	            int runLength=0;
                //append the data from vector into pages and then into file
	            for(vector<Record *>::iterator it=vec.begin();it!=vec.end();++it) {
                    if(!sortPage.Append(*it)) {
                        if(file.GetLength()==0)
                        {
                            file.AddPage(&sortPage,file.GetLength());
                            sortPage.EmptyItOut();
                            sortPage.Append(*it);
                            ++runLength;
                        }else{
                            file.AddPage(&sortPage,file.GetLength()-1);
                            sortPage.EmptyItOut();
                            sortPage.Append(*it);
                            ++runLength;
                        }
                    }
                }
                if(sortPage.numRecs!=0) {
                    if(file.GetLength()==0)
                    {
                        file.AddPage(&sortPage,file.GetLength());
                        sortPage.EmptyItOut();
                        ++runLength;
                    }else{
                        file.AddPage(&sortPage,file.GetLength()-1);
                        sortPage.EmptyItOut();
                        ++runLength;
                    }
                }
                vec.clear();
                vec.push_back(cRecord);
                myPage.EmptyItOut();
                myPage.Append(&record);
                pageCounter=0;
                ++runCounter;
                runL.push_back(runLength);
	        }
	        else {
	            myPage.EmptyItOut();
                myPage.Append(&record);
                vec.push_back(cRecord);
	        }
	    }
	}
//last partially full page needs to be sorted and added
	myPage.EmptyItOut();
	sortPage.EmptyItOut();
	if(!vec.empty()) {
		stable_sort(vec.begin(),vec.end(),SortRecordComparator(&sortorder));
		int runLength=0;
		for(vector<Record *>::iterator it=vec.begin();it!=vec.end();++it) {
			if(!sortPage.Append(*it)) {
				if(file.GetLength()==0) {
					file.AddPage(&sortPage,file.GetLength());
					sortPage.EmptyItOut();
					sortPage.Append(*it);
					++runLength;
				}else{
					file.AddPage(&sortPage,file.GetLength()-1);
					sortPage.EmptyItOut();
					sortPage.Append(*it);
					++runLength;
				}
			}
		}
		if(sortPage.numRecs!=0) {
			if(file.GetLength()==0) {
				file.AddPage(&sortPage,file.GetLength());
				sortPage.EmptyItOut();
				++runLength;
			}
			else {
				file.AddPage(&sortPage,file.GetLength()-1);
				sortPage.EmptyItOut();
				++runLength;
			}
		}
		++runCounter;
		runL.push_back(runLength);
	}
	cout<<"\n"<<count<<" records pumped into BigQ!!\n";
//refresh the vector
    for(vector<Record *>::iterator it=vec.begin();it!=vec.end();++it)
    	delete *it;
    vec.clear();
	file.Close();

    file.Open(1,fileName);

    //priority queue for pushing the data with least value, based on order comparison
	priority_queue<EnclosedRecord *, vector<EnclosedRecord *>, RecordComparator> myPQ(&sortorder);

	Page inputPage[runCounter];

	int whichPage[runCounter];

	vector<int> runStart;
	for(int i=0;i<runCounter;++i) {
        int num=0;
        for(vector<int>::iterator it=runL.begin();it!=runL.begin()+i;++it)
            num+=*it;
        runStart.push_back(num);
	}

	//filling up the buffers with first page of every run
	for(int i=0;i<runCounter;++i) {
        file.GetPage(&(inputPage[i]),runStart[i]);
		whichPage[i]=0;
	}
	//fetching the records from each first page
	for(int i=0;i<runCounter;++i) {
	    EnclosedRecord *myRR=new EnclosedRecord;
		myRR->runNum=i;
		inputPage[i].GetFirst(&(myRR->record));
		myPQ.push(myRR);
	}
	count=0;
	while(true) {
		//cout<<"still in";

		if(myPQ.empty())
			break;
		else {
			EnclosedRecord *chRR=myPQ.top();
			myPQ.pop();
			count++;
			int runNum = chRR->runNum;
			out->Insert(&(chRR->record));


			EnclosedRecord *buRR=new EnclosedRecord;
			if(inputPage[runNum].GetFirst(&(buRR->record))) {
			    buRR->runNum=runNum;
				myPQ.push(buRR);
			}
			//if the page is depleted, get a fresh page from file for the corresponding run
			else {
				++whichPage[runNum];
				if(runL[runNum]>whichPage[runNum]) {
				    file.GetPage(&(inputPage[runNum]),whichPage[runNum]+runStart[runNum]);
				    inputPage[runNum].GetFirst(&(buRR->record));
				    buRR->runNum=runNum;
				    myPQ.push(buRR);
				}
			}
		}
	}
	cout<<"\n"<<count<<" records pumped out of BigQ!\n";
    file.Close();
	//remove the temporary file and shutdown the output pipe
    remove(fileName);
	out->ShutDown ();
	cout<<"\nQueueShut!!";


}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {

	//hardware contains all the supporting data needed in the thread. Since only single argument can be passed to the thread,
	//everything is being encapsulated in single object
	Hardware *info=new Hardware();
	info->in=&in;
	info->out=&out;
	info->order=sortorder;
	info->runLength=runlen;
	pthread_t thread;
	//launch the thread
	pthread_create(&thread,NULL,&ThreadFunction,(void*)info);


}

BigQ::~BigQ () {
}
