/*
 * DBEngine.cc
 *
 *  Created on: May 3, 2018
 *      Author: adhikari
 */
#include <fstream>
#include <string>

#include "DBEngine.h"

using namespace std;

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern struct AttrList *attrs;
extern struct NameList *sort;
extern char *table, *file, *oldtable, *output;

bool DBEngine::Create() {

	//check whether table being create is already in the catalog file
	if(CheckCatalog(table))
		return false;

	//create a new metafile and update the DBFile type
	const char* nameMeta = (string(table)+".txt").c_str();
	ofstream meta(nameMeta);
	fType type = (sort? sorted : heap);
	meta<<type<<endl;

	OrderMaker order;

	//create a new relation in the catalog file
	//using table name and attribute information
	const char* types[3]={"Int", "Double", "String"};
	Type atypes[3]={Int, Double, String};
	//cout<<"\nhere1.5";
	ofstream catalog(catalog_path, std::ios_base::app);
	catalog << "BEGIN\n" <<table<< '\n' <<table<< ".tbl" << endl;
	int num=0;
	//cout<<"\nhere 1";
	AttrList* att = attrs;
	for (; ; ++num) {
		if(att==NULL)
			break;
		//cout<<"\nhere1.5";
		catalog<<att->name<<' '<<types[att->type]<< endl;
		att=att->next;
	}
	//cout<<"\nhere 2";
	catalog<<"END" << endl;
	Attribute* tempattrs = new Attribute[num];
	num=0;
	for (AttrList* att = attrs; att; att = att->next, ++num) {
		tempattrs[num].name = strdup(att->name);
		tempattrs[num].myType = atypes[att->type];
	}

	//create new schema using attribute information
	Schema sch("", num, tempattrs);
	if(type==sorted) {
		order.growFromParseTree(sort, &sch);
	}

	//encapsulate ordermaker and runlength into a structure for creation of DBFile
	struct SortInfo {
		OrderMaker * order;
		int length;
	}info;
	info.order=&order;
	info.length=500;

	//create DBFile
	DBFile dbfile;
	dbfile.Create((char*)(dbfile_dir+string(table)+".bin").c_str(), type, (void*)&info);
	dbfile.Close();
	delete[] tempattrs;
	catalog.close();
	return true;
}

bool DBEngine::CheckCatalog(const char* relName) {
	ifstream in (catalog_path);
	string str;
	while (getline(in, str)) {
		if (str == relName) {
			in.close();
			return true;
		}
	}
	in.close();
	return false;
}

bool DBEngine::Insert() {
	DBFile dbfile;
	char* path = new char[strlen(dbfile_dir)+strlen(oldtable)+4];
	strcpy(path,dbfile_dir);
	strcat(path, oldtable);
	strcat(path,".bin");
	//char* finalpath = (char*)((string(dbfile_dir)+string(path)).c_str());
	Schema sch(catalog_path, oldtable);
	//cout<<"\n"<<dbfile_dir;
	if(dbfile.Open(path)==0) {
		delete[] path;
		return false;
	}
	else {
		char* tblpath = new char[strlen(tpch_dir)+strlen(file)];
		strcpy(tblpath, tpch_dir);
		strcat(tblpath, file);
		//(char*)(string(tpch_dir)+string(file)).c_str();
		dbfile.Load(sch, tblpath);
		dbfile.Close();
		//delete[] path;
		return true;
	}

}

bool DBEngine::Drop() {
	ifstream in(catalog_path);
	ofstream out("cat.tmp");
	string line, temp="";
	string relation = oldtable;
	//cout<<"\n"<<catalog_path<<","<<oldtable;
	bool present=false, found=false;
	while(getline(in, line)) {
		//cout<<"\nhere!";
		if(line.empty())
			continue;
		//cout<<"\nline - "<<line;

		if(line==oldtable) {
			//cout<<"\nline - "<<line;
			present=true;
			found=true;
		}
		temp+=line+"\n";
		if(line=="END") {
			if(!found)
				out<<temp<<endl;
			found=false;
			temp.clear();
		}


	}
	rename("cat.tmp",catalog_path);
	in.close();
	out.close();

	if(present) {
		//cout<<"\ndeleting files from "<<dbfile_dir;
		remove((dbfile_dir+relation+".bin").c_str());

		remove((dbfile_dir+relation+".txt").c_str());
		return true;
	}
	return false;
}


