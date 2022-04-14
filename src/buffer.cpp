/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{

	BufMgr::BufMgr(std::uint32_t bufs)
		: numBufs(bufs)
	{
		bufDescTable = new BufDesc[bufs];

		for (FrameId i = 0; i < bufs; i++)
		{
			bufDescTable[i].frameNo = i;
			bufDescTable[i].valid = false;
		}

		bufPool = new Page[bufs];

		int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
		hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

		clockHand = bufs - 1;
	}

	BufMgr::~BufMgr()
	{
		for (FrameId i = 0; i < BufMgr::numBufs; i++) // for each Frame
		{
			if (bufDescTable[i].dirty == true)
			{
				bufDescTable[i].file->writePage(bufPool[i]); // write dirty page to disk
			}
			hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
		delete[] bufDescTable;
		delete[] bufPool;
		delete hashTable;
	}

	void BufMgr::advanceClock()
	{
		clockHand = (clockHand + 1) % numBufs;
	}

	void BufMgr::allocBuf(FrameId &frame)
	{
		// find a frame to allocate
		FrameId i;
		for (i = 0; i < numBufs; i++)
		{
			advanceClock();
			if (bufDescTable[clockHand].valid == false)
			{
				frame = clockHand;
				break;
			}
			else if (bufDescTable[clockHand].refbit == true)
			{
				bufDescTable[clockHand].refbit = false;
			}

			else if (bufDescTable[clockHand].pinCnt == 0)
			{
				// if the frame is dirty, write it back to disk
				if (bufDescTable[clockHand].dirty == true)
				{
					// bufDescTable[clockHand].file->writePage(bufDescTable[clockHand].pageNo, bufPool[clockHand]);
					bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
					bufDescTable[clockHand].dirty = false;
				}
				frame = clockHand;
				break;
			}
		}

		if (i == numBufs)
		{
			throw BufferExceededException();
		}
	}

	void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
	{
		FrameId frame;
		hashTable->lookup(file, pageNo, frame);

		if (frame == 0) // TODO: check this
		{
			// page not in buffer pool
			allocBuf(frame);
			bufPool[frame] = bufDescTable[frame].file->readPage(pageNo);
			hashTable->insert(file, pageNo, bufDescTable[frame].frameNo);
			bufDescTable[frame].Set(file, pageNo);
			page = &bufPool[frame];
		}
		else
		{
			// page is in buffer pool
			bufDescTable[frame].refbit = true;
			bufDescTable[frame].pinCnt++;
			page = &bufPool[frame];
		}
	}

	void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
	{
		for (FrameId i = 0; i < numBufs; i++)
		{
			if (bufDescTable[i].file == file && bufDescTable[i].pageNo == pageNo)
			{
				if (bufDescTable[i].pinCnt == 0)
				{
					throw PageNotPinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, i);
				}
				else
				{
					bufDescTable[i].pinCnt--;
					if (dirty == true)
					{
						bufDescTable[i].dirty = true;
					}
				}
				return;
			}
		}
		// TODO : Need to throw exception if file or Page not found throw BadBufferException();
	}

	void BufMgr::flushFile(const File *file)
	{
		for (FrameId i = 0; i < numBufs; i++)
		{
			if (bufDescTable[i].file == file)
			{
				if (bufDescTable[i].valid == false)
				{
					throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
				}
				if (bufDescTable[i].pinCnt > 0)
				{
					throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, i);
				}
				if (bufDescTable[i].dirty == true)
				{
					// bufDescTable[i].file->writePage(bufDescTable[i].pageNo, bufPool[i]);
					bufDescTable[i].file->writePage(bufPool[i]);
					bufDescTable[i].dirty = false;
				}
				hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
				bufDescTable[i].Clear();
			}
		}
	}

	void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
	{
		Page page1 = file->allocatePage();
		page = &page1;
		pageNo = page->page_number();

		FrameId frame;
		allocBuf(frame);
		bufPool[frame] = page1;
		hashTable->insert(file, pageNo, frame);
		bufDescTable[frame].Set(file, pageNo);
	}

	void BufMgr::disposePage(File *file, const PageId PageNo)
	{
		for (FrameId i = 0; i < numBufs; i++)
		{
			if (bufDescTable[i].file == file && bufDescTable[i].pageNo == PageNo)
			{
				if (bufDescTable[i].pinCnt > 0)
				{
					throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, i);
				}
				else
				{
					bufDescTable[i].Clear();
					hashTable->remove(file, PageNo);
					bufDescTable[i].file->deletePage(bufDescTable[i].pageNo);
				}
				return;
			}
		}
		// TODO : Need to throw exception if file or Page not found throw BadBufferException();
	}

	void BufMgr::printSelf(void)
	{
		BufDesc *tmpbuf;
		int validFrames = 0;

		for (std::uint32_t i = 0; i < numBufs; i++)
		{
			tmpbuf = &(bufDescTable[i]);
			std::cout << "FrameNo:" << i << " ";
			tmpbuf->Print();

			if (tmpbuf->valid == true)
				validFrames++;
		}

		std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	}

}
