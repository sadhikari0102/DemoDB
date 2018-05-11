#include "DBFile.h"
#include "Defs.h"
#include <cstdlib>
#include <cstdio>
#include <string.h>
#include <iostream>
#include <fstream>
#include "SortedDBFile.h"
#include "HeapDBFile.h"

/*
GenericDBFile::GenericDBFile() {

}

GenericDBFile::~GenericDBFile() {

}*/

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
	//initialize the object of GenericDBFile based on the file type supplied
    if(f_type == heap)
    	dbfile=new HeapDBFile;
    else if(f_type == sorted)
    	dbfile=new SortedDBFile;
    //populate filename information for metafile purposes
    dbfile->PopulateFileInfo(f_path);
    dbfile->PopulateFileType(f_type);

    //call the corresponding create method
    return dbfile->Create(f_path, f_type, startup);

}

int DBFile::Open (const char *f_path) {

    //getting the filename from the complete filepath to create metadata file
	//cout<<"\nf_path value"<<f_path;
	string path(f_path);
	int index_slash = path.find_last_of("/");
    int index_dot = path.find_last_of(".");
    string filename = path.substr(index_slash+1,index_dot-index_slash-1);
    string fpath;
    if(index_slash>=0)
    	fpath = path.substr(0,index_slash+1);
    else
        fpath="";

    //reading the metadata file for filetype and related data
    ;
    string fname = fpath+filename+".txt";
    //cout<<"\nOpening Metafile -"<< fname;
    ifstream metafile(fname.c_str());

    if(!metafile.is_open()) {
    	cout<<"\nUnable to open the metafile, "<<fname<<"! Please use create() function to create a fresh file.\n";
    	return 0;
    }

    // reading the file type
    string ftype;
    if(!metafile.eof()) {
    	getline(metafile,ftype);
    	//cout<<"\nFile type - "<<ftype;
    }
    else {
    	cout<<"\nMeta file is empty, Aborting!!\n";
    	metafile.close();
    	return 0;
    }
    metafile.close();
    //type-casting String filetype to Enum format
    fType file_type = static_cast<fType>(atoi(ftype.c_str()));
	if(file_type==heap)
		dbfile=new HeapDBFile();
	else if(file_type==sorted)
		dbfile=new SortedDBFile();
	dbfile->PopulateFileInfo(f_path);
	dbfile->PopulateFileType(file_type);
	return dbfile->Open(f_path, fname);

}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
	dbfile->Load(f_schema, loadpath);
}

void DBFile::MoveFirst () {
	dbfile->MoveFirst();
}

void DBFile::Add (Record &rec) {
	dbfile->Add(rec);
}

int DBFile::Close () {
	return dbfile->Close();
}


int DBFile::GetNext (Record &fetchme) {
	return dbfile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	return dbfile->GetNext(fetchme, cnf, literal);
}


int GenericDBFile::Create(const char *f_path, fType f_type, void *startup) {

	//creating the file on the disk
	file.Open(0, (char*)f_path);
	page_index=0;
	mode ='r';
	//cout<<"\nFile created in read mode\n";
	return 1;
}

int GenericDBFile::Open(const char *f_path, string fname) {
	//cout<<"\n opening "<<f_path;
    file.Open(1, (char*)f_path);
	//Open the file with first page automatically in the buffer
    if(file.GetLength()>0) {
		file.GetPage(&buffer, 0);
		page_index=0;
	}
	/*else {
		//cout<<"\nFile "<<f_path<<" open with length 0!!";
		return 0;
	}*/
	//Initialize the mode as READ by default
	mode='r';
	//cout<<"\nFile "<<f_path<<" opened in read mode\n";

	return 1;
}

void GenericDBFile::Load (Schema &f_schema, const char *loadpath) {
    //load the table file(*.tbl)
	FILE *tableFile = fopen (loadpath, "r");
	Record temp;
	//keep reading till the end of file
	while (temp.SuckNextRecord (&f_schema, tableFile) == 1) {
		//will call the corresponding Add() method based on object type
		Add(temp);
	}
}

void GenericDBFile::MoveFirst () {
	//load the first page into memory in case file is not empty
	if(file.GetLength()!=0) {
		file.GetPage(&buffer, 0);
		page_index=0;
	}
	//cout<<"\nFile "<<fullname<<" has length "<<file.GetLength();
}

int GenericDBFile::GetNext (Record &fetchme) {

	// if it fails to get the record
	if (buffer.GetFirst(&fetchme) == 0) {

		//get the next page
		buffer.EmptyItOut();
		++page_index;

		// if end of the file is reached, cannot getNext() anymore
		if (page_index>file.GetLength()-2) {

			//cout<<"\nEnd of file reached!!";
			return 0;
		}
		file.GetPage(&buffer, page_index);
		buffer.GetFirst(&fetchme);
	}

	return 1;
}

int GenericDBFile::Close() {
	//cout<<"File "<<fullname<<" closing!";
	return file.Close();
}

void GenericDBFile::PopulateFileInfo(const char *f_path) {
    //getting the filename from the complete filepath to create metadata file
	string path(f_path);
    int index_slash = path.find_last_of("/");
    int index_dot = path.find_last_of(".");
    filename = path.substr(index_slash+1,index_dot-index_slash-1);
    //if path has no directory
    if(index_slash>=0)
    	filepath = path.substr(0,index_slash+1);
    else
    	filepath="";
    fullname = path;
}

void GenericDBFile::PopulateFileType(fType type) {
	file_type = type;
}

