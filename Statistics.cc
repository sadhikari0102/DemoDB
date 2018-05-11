
#include "Statistics.h"

#include <string.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>


Statistics::Statistics() {

}

Statistics::Statistics(Statistics &copyMe) {
	map<string, RelationData>::iterator it;
	for (it = copyMe.dbMap.begin() ; it!=copyMe.dbMap.end(); it++) {
		string relname = it->first;
		int tupleCount = it->second.getTupleCount();
		//map<string, int> attr = it->second.getAttributes();
		RelationData newrel(relname, tupleCount);
		map<string, int>::iterator it_attr;
		map<string, int> attr = it->second.getAttributes();
		for (it_attr = attr.begin() ; it_attr!= attr.end(); it_attr++) {
			string attr_name = it_attr->first;
			int noOfDistinct = it_attr->second;
			newrel.addAttribute(attr_name, noOfDistinct);
		}
		dbMap.insert(pair<string, RelationData>(relname,newrel));

	}
}

Statistics::~Statistics() {
}

void Statistics::AddRel(char *relName, int numTuples) {
	string name(relName);
	RelationData rel(name, numTuples);
	dbMap.insert(pair<string, RelationData>(name, rel));
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
	string name(relName);
	map<string, RelationData>::iterator it;
	it = dbMap.find(name);
	if (it == dbMap.end()) {
		cout<<"\nERROR!! Relation not present in the map!!" ;
	}
	else {
		if (numDistincts == -1)
			numDistincts = it->second.getTupleCount();
		it->second.addAttribute(attName, numDistincts);
	}
}

void Statistics::CopyRel(char *oldName, char *newName) {
	string source(oldName) ;
	string dest(newName);
	//find the old relation
	map<string, RelationData>::iterator it;
	it = dbMap.find(source);
	if (it == dbMap.end()) {
		cout<<"\nERROR!! Original relation not present in the map!!" ;
	}
	else {
		int noOfTuples = it->second.getTupleCount();
		map<string, int> attr = it->second.getAttributes();
		RelationData newRel(dest, noOfTuples, attr);
		dbMap.insert(pair<string, RelationData>(dest, newRel));
	}
}

void Statistics::Read(char *fromWhere) {
	FILE *data=fopen(fromWhere, "r");
	if (data == NULL) {
		data = fopen(fromWhere, "w");
		fprintf(data, "eof\n");
		fclose(data);
		cout<<"\nFile was not found, empty file created!!\n";
		data = fopen(fromWhere, "r");
	}
	char *temp = new char[200];
	fscanf(data,"%s",temp);
	while(strcmp(temp,"eof")) {
		if(strcmp(temp,"relName")) {

		}
		else {
			fscanf(data,"%s",temp);
			string rname(temp);

			//test
			//cout<<"\n"<<rname;

			fscanf(data,"%s",temp);
			int tups = atoi(temp);

			//test
			//cout<<"\n"<<tups;

			RelationData newRel(rname,tups);
			newRel.numOfJoins=1;
			//skipping "attributes"
			fscanf(data,"%s",temp);
			fscanf(data,"%s",temp);

			while(strcmp(temp,"relName") && strcmp(temp,"eof")) {
				fscanf(data,"%s",temp);
				string aname(temp);

				//test
				//cout<<"\n"<<aname;

				fscanf(data,"%s",temp);
				fscanf(data,"%s",temp);
				int unique = atoi(temp);

				//test
				//cout<<"\n"<<unique;

				newRel.attributes.insert(pair<string,int>(aname,unique));
				fscanf(data,"%s",temp);
			}
			dbMap.insert(pair<string,RelationData>(rname,newRel));
		}
	}

	fclose(data);

}

void Statistics::Write(char *fromWhere) {
	FILE *data=fopen(fromWhere, "w");
	if (data == NULL) {
		cout<<"\nError opening file containing Statistics object!!\n";
		return;
	}
	else {
		//write the object to the file
		for(map<string, RelationData>::iterator it=dbMap.begin();it!=dbMap.end();++it) {

			string relName(it->first);
			char *out = new char[relName.length() +1];
			strcpy(out, relName.c_str());
			fprintf(data, "\nrelName\n%s\n%d\n", out, it->second.getTupleCount());
			fprintf(data, "\nattributes\n");
			//writing the attributes of the file
			map<string, int>::iterator it_attr;
			map<string,int> attr= it->second.getAttributes();
			for(it_attr = attr.begin(); it_attr!=attr.end();++it_attr) {

				char *att = new char[it_attr->first.length()+1];
				strcpy(att, it_attr->first.c_str());
				fprintf(data, "\nrattrName\n%s", att);
				fprintf(data, "\ndistinct\n%d", it_attr->second);

			}
		}

		fprintf(data, "\neof\n");
	}
	fclose(data);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
	double estimate = Estimate(parseTree, relNames, numToJoin);

	int count=0;
	while(parseTree!=NULL) {
		struct OrList *ortree = parseTree->left;
		map<string, int>::iterator previous;
		double factorOr=0.0;
		while(ortree!=NULL) {
			//cout<<"\n"<<ortree->left->left->value;
			//cout<<"\n"<<ortree->left->right->value;


			//check whether the CNF contains both operands as attribute names
			if(ortree->left->left->code == 3 && ortree->left->right->code == 3 ) {
				map<string, int>::iterator whichAtt1, whichAtt2;
				map<string, RelationData>::iterator whichRel1, whichRel2;

				//same as estimate function, find the relation and attribute details based on
				//the type of notaion in the comparison expression

				string relName, attName;
				bool flag=false;
				if(FindRelation(ortree->left->left->value, relName, attName, relNames, numToJoin)) {
					whichRel1=dbMap.find(relName);
					whichAtt1=whichRel1->second.attributes.find(attName);

				}

				if(FindRelation(ortree->left->right->value, relName, attName, relNames, numToJoin)) {
					whichRel2=dbMap.find(relName);
					whichAtt2=whichRel2->second.attributes.find(attName);
					flag=true;
				}

				if(!flag) {
					for(map<string, RelationData>::iterator it = dbMap.begin(); it!=dbMap.end(); ++it) {
						map<string, int>::iterator tempAtt = it->second.attributes.find(ortree->left->left->value);
						if(tempAtt!=it->second.attributes.end()) {
							whichRel1 = it;
							whichAtt1=tempAtt;
						}

						tempAtt = it->second.attributes.find(ortree->left->right->value);
						if(tempAtt !=it->second.attributes.end()) {
							whichRel2 = it;
							whichAtt2=tempAtt;
						}
					}
				}

				//combine the relations to create a new one as a join of the two
				string newName = whichRel1->first + "-" + whichRel2->first;
				RelationData newRelation(newName, estimate);
				newRelation.numOfJoins = numToJoin;
				map<string, int> attr = whichRel1->second.attributes;
				for(map<string, int>::iterator it = attr.begin(); it!=attr.end();++it)
					newRelation.attributes.insert(*it);

				attr = whichRel2->second.attributes;
				for(map<string, int>::iterator it = attr.begin(); it!=attr.end();++it)
					newRelation.attributes.insert(*it);

				//remove the old relations which are being combined
				dbMap.erase(whichRel1);
				dbMap.erase(whichRel2);

				//add the new relation to the statistics object
				dbMap.insert(pair<string,RelationData>(newName, newRelation));

			}

			//if only left operand is the attribute name (case where comparing with a value)
			else {
				map<string, int>::iterator whichAtt;
				map<string, RelationData>::iterator whichRel;

				string relName, attName;
				bool flag=false;
				if(FindRelation(ortree->left->left->value, relName, attName, relNames, numToJoin)) {
					whichRel=dbMap.find(relName);
					whichAtt=whichRel->second.attributes.find(attName);
					flag=true;
				}
				if(!flag) {
					for(map<string, RelationData>::iterator it = dbMap.begin(); it!=dbMap.end(); ++it) {
						whichAtt = it->second.attributes.find(ortree->left->left->value);
						if(whichAtt!=it->second.attributes.end()) {
							whichRel = it;
							break;
						}
					}
				}
				whichRel->second.numOfTuples=estimate;

			}
			ortree = ortree->rightOr;
		}
		parseTree = parseTree->rightAnd;
	}

}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {

	if(parseTree==NULL) {
		if(numToJoin>1) {
			//cout<<"\nCannot join "<<numToJoin<<" relations without CNF!!";
			return -1;
		}

		if(dbMap.find(relNames[0])==dbMap.end()) {
			cout<<"\nRelation not found in the existing Data!!";
			return -1;
		}
		//cout<<"\n estimate : "<<dbMap[relNames[0]].numOfTuples;
		return dbMap[relNames[0]].numOfTuples;
	}
	double tuples=0.0;
	double factor=1.0;
	int count=0;
	while(parseTree!=NULL) {
		struct OrList *ortree = parseTree->left;
		map<string, int>::iterator previous;
		double factorOr=0.0;
		while(ortree!=NULL) {
			//cout<<"\n"<<ortree->left->left->value;
			//cout<<"  ,  "<<ortree->left->right->value;

			//check whether the CNF contains both operands as attribute names
			if(ortree->left->left->code == 3 && ortree->left->right->code == 3 ) {

				map<string, int>::iterator whichAtt1, whichAtt2;
				map<string, RelationData>::iterator whichRel1, whichRel2;

				//if the comparison expression contains '.' notation, fetch the relation name and attribute name
				string relName, attName;
				bool flag=false;
				if(FindRelation(ortree->left->left->value, relName, attName, relNames, numToJoin)) {
					whichRel1=dbMap.find(relName);
					whichAtt1=whichRel1->second.attributes.find(attName);

				}

				if(FindRelation(ortree->left->right->value, relName, attName, relNames, numToJoin)) {
					whichRel2=dbMap.find(relName);
					whichAtt2=whichRel2->second.attributes.find(attName);
					flag=true;
				}

				//if comparison expression only states attribute names, fetch the relation and attribute details
				//from the statistics object
				if(!flag) {
					for(map<string, RelationData>::iterator it = dbMap.begin(); it!=dbMap.end(); ++it) {
						map<string, int>::iterator tempAtt = it->second.attributes.find(ortree->left->left->value);
						if(tempAtt!=it->second.attributes.end()) {
							whichAtt1=tempAtt;
							whichRel1 = it;
						}

						tempAtt = it->second.attributes.find(ortree->left->right->value);
						if(tempAtt!=it->second.attributes.end()) {
							whichAtt2=tempAtt;
							whichRel2 = it;
						}
					}
				}

				if(whichRel1->first==whichRel2->first) {
					cout<<"\n relation name - "<<whichRel1->first;
					ortree = ortree->rightOr;
					continue;
				}
				//cout<<"\n"<<whichRel1->first<<"."<<whichAtt1->first;
				//cout<<"\n"<<whichRel2->first<<"."<<whichAtt2->first;

				//find the greater of the two tuple set
				double unique = (whichAtt1->second >= whichAtt2->second)?(double)whichAtt1->second:(double)whichAtt2->second;
				tuples = (count==0)
						?(double)whichRel1->second.numOfTuples*(double)whichRel2->second.numOfTuples/unique
						:tuples*(double)whichRel2->second.numOfTuples/unique;
				count++;
			}
			//if only left operand is the attribute name (case where comparing with a value)
			else {

				map<string, int>::iterator whichAtt;
				map<string, RelationData>::iterator whichRel;

				//if the comparison expression contains '.' notation, fetch the relation name and attribute name
				string relName, attName;
				bool flag=false;
				if(FindRelation(ortree->left->left->value, relName, attName, relNames, numToJoin)) {
					whichRel=dbMap.find(relName);
					whichAtt=whichRel->second.attributes.find(attName);
					flag=true;
				}
				//if comparison expression only states attribute names, fetch the relation and attribute details
				//from the statistics object
				if(!flag) {

					for(map<string, RelationData>::iterator it = dbMap.begin(); it!=dbMap.end(); ++it) {
						whichAtt = it->second.attributes.find(ortree->left->left->value);
						if(whichAtt!=it->second.attributes.end()) {
							whichRel = it;
							break;
						}
					}
				}

				//for '=' operation, no.of unique values derive the factor, for other comparisons,
				//a probability of 1/3rd is taken
				double temp = (ortree->left->code == 7)?1.0/whichAtt->second:1.0/3.0;
				if(previous!=whichAtt)
					factorOr = temp+factorOr-(factorOr*temp);
				else
					factorOr = factorOr + temp;

				previous = whichAtt;
				tuples=(tuples==0)?(double)(whichRel->second.numOfTuples):tuples;
			}
			ortree = ortree->rightOr;
		}
		//combining the effect of each OR factor
		if(factorOr!=0.0)
			factor = factor * factorOr;
		parseTree = parseTree->rightAnd;
	}
	tuples*=factor;
	//cout<<"\n estimate : "<<tuples;
	return tuples;
}

bool Statistics::FindRelation(string operand, string& relName, string& attName, char **relNames, int numToJoin) {

	int index=operand.find(".");
	if(index!=string::npos) {
		relName = operand.substr(0,index);
		attName = operand.substr(index+1);
		int i;
		//cout<<"\n"<<relName<<"\n";

		//check if relation in the CNF is present in the set being joined
		for(i=0;i<numToJoin;i++) {
			if(relName==relNames[i])
				break;
		}
		if(i==numToJoin) {
			cout<<"\nError!! "<<relName<<" not part of the set! Aborting!!";
			exit(0);
		}



		//check if relation in the CNF is part of Statistics object
		map<string,RelationData>::iterator it;
		for(it=dbMap.begin();it!=dbMap.end();++it) {
			int pos = it->first.find(relName);
			if(pos!=string::npos) {
				//check if it is stand-alone relation or part of a joined set
				if(pos==0) {

					if(!strcmp(it->first.c_str(),relName.c_str())) {
						break;
					}
					if(it->first.at(pos+relName.length())=='-') {
						relName = it->first;
						break;
					}
				}
				//fetch the name of joined set in case it's not stand-alone
				else {
					if(it->first.at(pos-1)=='-') {
						relName = it->first;
						break;
					}
				}
			}
		}


		if(it != dbMap.end()) {
			//check if said attribute is part of the relation
			map<string, int> attr = dbMap[relName].attributes;
			if(attr.find(attName) != attr.end()) {

			}
			else {
				cout<<"\nError!! "<<attName<<" not part of the Relation "<<relName<<" ! Aborting!!";
				exit(0);
			}
		}
		else {
			cout<<"\nError!! "<<relName<<" not part of the Statistics object! Aborting!!";
			exit(0);
		}


		return true;
	}
	return false;
}

void  Statistics::Print() {
	map<string, RelationData>::iterator rit;
	for(rit=dbMap.begin();rit!=dbMap.end();++rit) {
		cout<<"\n"<<rit->first;
		cout<<"\n"<<rit->second.numOfJoins;
		cout<<"\n"<<rit->second.numOfTuples;
		map<string, int> attr = rit->second.attributes;
		for(map<string,int>::iterator it = attr.begin();it!=attr.end(); ++it) {
			cout<<"\n"<<it->first<<"-";
			cout<<it->second;
		}
	}
}





