/**
 * @author Adithya Anand (A59010781, UCSD), Mohit Shah (A59005444, UCSD)
 * @brief BadgerDB which maintains, initialises and provides functionality to the buffer manager and the buffer pool,
 * along with defining the clock replacement algorithm.
 * @section LICENSE
 * Partial copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

/**
 * @brief Class for maintaining badgerdb
 */
namespace badgerdb
{

	/**
	 * @brief Constructor for BufDesc: Creates a new BufDesc object as a table and
	 * sets the values.
	 * Also creates a pool of frames for the buffer manager to hold pages
	 * Also creates a hash table to store the frames
	 *
	 * @param bufs Number of buffer frames to be created
	 */
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

	/**
	 * @brief Destructor for BufMgr: Deallocate all relevant memory
	 */
	BufMgr::~BufMgr()
	{
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

	/**
	 * @brief Increment the clock hand as part of the clock algorithm
	 */
	void BufMgr::advanceClock()
	{
		clockHand = (clockHand + 1) % numBufs;
	}

	/**
	 * @brief Allocate a new free buffer frame.
	 *
	 * @param frame  The frame ID which gets determined after allocation, returned through this variable.
	 * @throws BufferExceededException If no such buffer is found which can be allocated
	 */
	void BufMgr::allocBuf(FrameId &frame)
	{
		// find a frame to allocate
		bool foundUnpin = false;
		FrameId start = clockHand;
		while (1)
		{
			advanceClock();

			if (bufDescTable[clockHand].valid == false)
			{
				// found a frame to allocate since valid bit is false
				frame = clockHand;
				return;
			}
			else if (bufDescTable[clockHand].pinCnt == 0)
			{
				foundUnpin = true; // found a potential frame to allocate since pin count is 0
				if (bufDescTable[clockHand].refbit == false)
				{
					// if the frame is dirty, write it back to disk
					if (bufDescTable[clockHand].dirty == true)
					{
						bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
						hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
						bufDescTable[clockHand].Clear();
					}

					frame = clockHand;
					return;
				}
			}
			bufDescTable[clockHand].refbit = false;
			if (clockHand == start) // if we've looped around
			{
				if (foundUnpin == false) // Throw exception if all pages are pinned
				{
					throw BufferExceededException();
				}
				else
					foundUnpin = false;
			}
		}
	}

	/**
	 * Reads the given page from the file into a frame and returns the pointer to page.
	 * If the requested page is already present in the buffer pool pointer to that frame is returned
	 * otherwise a new frame is allocated from the buffer pool for reading the page.
	 *
	 * @param file   	File object
	 * @param PageNo    Page number in the file to be read
	 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
	 * @throws InvalidPageException if the page requested does not exist in the file
	 */
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

	/**
	 * Unpin a page from memory since it is no longer required for it to remain in memory.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 * @param dirty		True if the page to be unpinned needs to be marked dirty
	 * @throws  PageNotPinnedException If the page is not already pinned
	 */
	void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
	{
		FrameId frame;
		try
		{
			hashTable->lookup(file, pageNo, frame);

			if (bufDescTable[frame].pinCnt == 0) // Throw exception if Page is already unpinned
			{
				throw PageNotPinnedException(bufDescTable[frame].file->filename(), pageNo, frame);
			}

			bufDescTable[frame].pinCnt--;
			if (dirty == true)
			{
				bufDescTable[frame].dirty = true;
			}
		}
		catch (HashNotFoundException e)
		{
		}
	}

	/**
	 * Writes out all dirty pages of the file to disk.
	 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
	 * Otherwise Error returned.
	 *
	 * @param file   	File object
	 * @throws  PagePinnedException If any page of the file is pinned in the buffer pool
	 * @throws BadBufferException If any frame allocated to the file is found to be invalid
	 */
	void BufMgr::flushFile(const File *file)
	{
		// Check for each frame belonging to the file being flushed in the pool
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
				// if page in frame is dirty, write it back to disk
				if (bufDescTable[i].dirty == true)
				{
					// bufDescTable[i].file->writePage(bufDescTable[i].pageNo, bufPool[i]);
					bufDescTable[i].file->writePage(bufPool[i]);
					bufDescTable[i].dirty = false;
				}
				// remove the page from the hash table and out of the buffer pool
				hashTable->remove(bufDescTable[i].file, bufDescTable[i].pageNo);
				bufDescTable[i].Clear();
			}
		}
	}

	/**
	 * Allocates a new, empty page in the file and returns the Page object.
	 * The newly allocated page is also assigned a frame in the buffer pool.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
	 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
	 */
	void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
	{
		Page temp_page = file->allocatePage();

		FrameId frame;
		allocBuf(frame);
		bufPool[frame] = temp_page;

		page = &bufPool[frame];
		pageNo = temp_page.page_number();

		// set and insert the page
		bufDescTable[frame].Set(file, pageNo);
		hashTable->insert(file, pageNo, frame);
	}

	/**
	 * Delete page from file and also from buffer pool if present.
	 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
	 *
	 * @param file   	File object
	 * @param PageNo  Page number
	 */
	void BufMgr::disposePage(File *file, const PageId PageNo)
	{
		FrameId frame;
		try
		{
			// find the frame ID of the page to dispose, if available in buffer pool
			hashTable->lookup(file, PageNo, frame);

			// check if the page is pinned
			if (bufDescTable[frame].pinCnt > 0)
			{
				throw PagePinnedException(file->filename(), PageNo, frame);
			}

			// clear bufDescTable's frame of the page since it's getting disposed from the buffer pool
			bufDescTable[frame].Clear();
			// remove from hash table
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
