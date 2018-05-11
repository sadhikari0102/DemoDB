#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <string>


typedef enum {heap, sorted, tree} fType;

class GenericDBFile;
class HeapDBFile;

class DBFile {

//Assignment 1 changes
private:

	GenericDBFile* dbfile;
	DBFile(const DBFile&);
	DBFile& operator=(const DBFile&);

public:
	DBFile() {

	}
	~DBFile() {

	}

	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};


//Internal base class to be instantiated from main DBFile object,
//to separate the functionality of different types of file (Heap, Sorted, B+)
class GenericDBFile {
protected:
	Page buffer;
	File file;
	int page_index;
	char mode;

	fType file_type;
	string filepath;
	string fullname;
	string filename;

public:
	GenericDBFile() {}
	virtual ~GenericDBFile() {}

	virtual int Create (const char *f_path, fType f_type, void *startup);
	virtual int Open (const char *f_path, string fname);
	virtual void Add (Record& addme) {};
	virtual void Load (Schema &f_schema, const char *loadpath);
	virtual int Close ();


	virtual void MoveFirst ();
	virtual int GetNext (Record &fetchme);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) {};


	void PopulateFileInfo(const char *f_path);
	void PopulateFileType(fType type);

private:
	GenericDBFile(const GenericDBFile&);
	GenericDBFile& operator=(const GenericDBFile&);

};

#endif
