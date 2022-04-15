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
		// printf("inside destructor of BufMgr\n");
		for (FrameId i = 0; i < BufMgr::numBufs; i++) // for each Frame
		{
			if (bufDescTable[i].file != NULL && bufDescTable[i].file->isOpen(bufDescTable[i].file->filename()) && bufDescTable[i].dirty == true)
			{
				bufDescTable[i].file->writePage(bufPool[i]); // write dirty page to disk
				bufDescTable[i].Clear();
			}
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

		bool foundUnpin = false;
		FrameId start = clockHand;
		// printf("start: %d\n", start);
		while (1)
		{
			advanceClock();

			if (bufDescTable[clockHand].valid == false)
			{
				// printf("found unpinned buffer: %d\n", clockHand);
				frame = clockHand;
				return;
			}
			else if (bufDescTable[clockHand].pinCnt == 0)
			{
				foundUnpin = true;
				if (bufDescTable[clockHand].refbit == false)
				{
					// if the frame is dirty, write it back to disk
					if (bufDescTable[clockHand].dirty == true)
					{
						// bufDescTable[clockHand].file->writePage(bufDescTable[clockHand].pageNo, bufPool[clockHand]);
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
						hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
						bufDescTable[clockHand].Clear();
					}
					// printf("found unpinned buffer: %d\n", clockHand);
					frame = clockHand;
					return;
				}
			}
			bufDescTable[clockHand].refbit = false;
			if (clockHand == start) // if we've looped around
			{
				if (foundUnpin == false)
				{
					// printf("ERROR: no unpinned buffers available with clockhand: %d\n", clockHand);
					throw BufferExceededException();
				}
				else
					foundUnpin = false;
			}
		}
	}

	void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
	{
		FrameId frame;
		try
		{
			hashTable->lookup(file, pageNo, frame);

			// page is in buffer pool
			bufDescTable[frame].refbit = true;
			bufDescTable[frame].pinCnt++;
			page = &bufPool[frame];
		}
		catch (HashNotFoundException e)
		{

			// page not in buffer pool
			allocBuf(frame);
			bufPool[frame] = file->readPage(pageNo);
			bufDescTable[frame].Set(file, pageNo);
			hashTable->insert(file, pageNo, frame);
			page = &bufPool[frame];
		}
	}

	void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
	{
		FrameId frame;
		try
		{
			hashTable->lookup(file, pageNo, frame);

			if (bufDescTable[frame].pinCnt == 0)
			{
				throw PageNotPinnedException(bufDescTable[frame].file->filename(), pageNo, frame);
			}

			bufDescTable[frame].pinCnt--;
			// printf("New pincnt : %d\n", bufDescTable[frame].pinCnt);
			if (dirty == true)
			{
				bufDescTable[frame].dirty = true;
			}
		}
		catch (HashNotFoundException e)
		{
		}
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
		Page temp_page = file->allocatePage();

		FrameId frame;
		allocBuf(frame);
		bufPool[frame] = temp_page;

		page = &bufPool[frame];
		pageNo = temp_page.page_number();

		bufDescTable[frame].Set(file, pageNo);
		hashTable->insert(file, pageNo, frame);
	}

	void BufMgr::disposePage(File *file, const PageId PageNo)
	{
		FrameId frame;
		try
		{
			hashTable->lookup(file, PageNo, frame);

			if (bufDescTable[frame].pinCnt > 0)
			{
				throw PagePinnedException(file->filename(), PageNo, frame);
			}
			bufDescTable[frame].Clear();
			hashTable->remove(file, PageNo);
		}
		catch (HashNotFoundException e)
		{
		}

		file->deletePage(PageNo);
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
