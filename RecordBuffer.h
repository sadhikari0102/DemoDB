/*
 * RecordBuffer.h
 *
 *  Created on: Mar 26, 2018
 *      Author: adhikari
 */

#ifndef RECORDBUFFER_H_
#define RECORDBUFFER_H_
#include "Record.h"

class RecordBuffer {
public:
	size_t size, capacity;
	Record* data;
	size_t count;

public:
	RecordBuffer(size_t pages);
	~RecordBuffer();
	int Insert(Record &addme);
	void Refresh();
	Record* GetData();
};



#endif /* RECORDBUFFER_H_ */
