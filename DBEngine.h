/*
 * DBEngine.h
 *
 *  Created on: May 3, 2018
 *      Author: adhikari
 */

#ifndef DBENGINE_H_
#define DBENGINE_H_

#include "DBFile.h"
#include "ParseTree.h"

class DBEngine {
public:
	bool Create();
	bool Insert();
	bool Drop();
	bool CheckCatalog(const char* relName);
};



#endif /* DBENGINE_H_ */
