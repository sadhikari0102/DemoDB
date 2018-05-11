
#ifndef STATISTICS_H_
#define STATISTICS_H_
#include <map>
#include <string>

#include "ParseTree.h"
using namespace std;

class RelationData {
	friend class Statistics;
	string name ;
	int numOfJoins;
	int numOfTuples;
	map<string, int> attributes ;

public:
	RelationData () {
		numOfTuples = 0;
	}

	RelationData (string relname, int tupleNum, map<string,int> attr) {
		name = relname ;
		numOfJoins=1;
		numOfTuples = tupleNum ;

		map<string, int>::iterator it_attr;
		//map<string, int> attr = it->second.getAttributes();
		for (it_attr = attr.begin() ; it_attr!= attr.end(); it_attr++) {
			string attr_name = it_attr->first;
			int noOfDistinct = it_attr->second;
			addAttribute(attr_name, noOfDistinct);
		}
	}

	RelationData (string relname, int tupleNum) {
			name = relname ;
			numOfTuples = tupleNum ;
			numOfJoins=1;
	}

	string getName() {
		return name;
	}

	void setName(string relname) {
		name = relname;
	}

	void setTupleCount(int tupleNum) {
		numOfTuples = tupleNum;
	}
	int getTupleCount() {
		return numOfTuples;
	}

	map<string, int> getAttributes() {
		return attributes;
	}

	void addAttribute (string attrName, int numDistincts) {
		attributes.insert(pair<string, int> (attrName, numDistincts));
	}
	//add attrb

};

class Statistics {

	map<string, RelationData> dbMap;

public:

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	bool FindRelation(string operand, string& relName, string& attName, char **relNames, int numToJoin);
	void Print();

};




#endif /* STATISTICS_H_ */
