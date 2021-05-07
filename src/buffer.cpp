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

namespace badgerdb {

    BufMgr::BufMgr(std::uint32_t bufs)
        : numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

        for (FrameId i = 0; i < bufs; i++) {
            bufDescTable[i].frameNo = i;
            bufDescTable[i].valid = false;
        }

        bufPool = new Page[bufs];

        int hashTblSize = ((((int) (bufs * 1.2)) * 2) / 2) + 1;
        hashTable = new BufHashTbl(hashTblSize);  // allocate the buffer hash table

        clockHand = bufs - 1;
    }

    BufMgr::~BufMgr() {
    }

    void BufMgr::advanceClock() {
        clockHand = (clockHand + 1) % numBufs;
    }

    void BufMgr::allocBuf(FrameId &frame) {
        uint32_t pinnedCnt = 0;
        while (true) {
            // TODO: PIN
            BufDesc &bufferDesc = bufDescTable[clockHand];
            if (bufferDesc.pinCnt > 0) {
                pinnedCnt++;
                if (pinnedCnt == numBufs) {
                    throw BufferExceededException();
                }
            } else if (!bufferDesc.refbit) {
                if (bufferDesc.dirty) {
                    bufferDesc.file->writePage(bufPool[clockHand]);
                }
                hashTable->remove(bufferDesc.file, bufferDesc.pageNo);
                bufferDesc.Clear();
                frame = bufferDesc.frameNo;
                return;
            }
            bufferDesc.refbit = false;
            advanceClock();
        }
    }

    void BufMgr::readPage(File *file, const PageId pageNo, Page *&page) {
        FrameId frameId;
        try {
            hashTable->lookup(file, pageNo, frameId);
        } catch (const HashNotFoundException &e) {
            allocBuf(frameId);
            bufPool[frameId] = file->readPage(pageNo);
            hashTable->insert(file, pageNo, frameId);
            bufDescTable[frameId].Set(file, pageNo);
            page = &bufPool[frameId];
            return;
        }
        bufDescTable[frameId].refbit = true;
        bufDescTable[frameId].pinCnt++;
        page = &bufPool[frameId];
        return;
    }

    void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty) {
        FrameId frameId;
        try {
            hashTable->lookup(file, pageNo, frameId);
        } catch (const HashNotFoundException &e) {
            return;
        }
        BufDesc &bufferDesc = bufDescTable[frameId];
        if (bufferDesc.pinCnt > 0) {
            bufferDesc.pinCnt--;
        } else {
            throw PageNotPinnedException(file->filename(), pageNo, frameId);
        }
        if (dirty) {
            bufferDesc.dirty = true;
        }
    }

    void BufMgr::flushFile(const File *file) {
        for (FrameId i = 0; i < numBufs; i++) {
            BufDesc &bufferDesc = bufDescTable[i];
            if (bufferDesc.file == file) {
                if (bufferDesc.pinCnt > 0) {
                    throw PagePinnedException(file->filename(), bufferDesc.pageNo, bufferDesc.frameNo);
                }
                if (!bufferDesc.valid) {
                    throw BadBufferException(bufferDesc.frameNo, bufferDesc.dirty, bufferDesc.valid, bufferDesc.refbit);
                }
                if (bufferDesc.dirty) {
                    bufferDesc.file->writePage(bufPool[i]);
                    bufferDesc.dirty = false;
                }
                hashTable->remove(file, bufferDesc.pageNo);
                bufferDesc.Clear();
            }
        }
    }

    void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page) {
        Page filePage = file->allocatePage();
        pageNo = filePage.page_number();
        FrameId frameId;
        allocBuf(frameId);
        page = &bufPool[frameId];
        hashTable->insert(file, pageNo, frameId);
        bufDescTable->Set(file, pageNo);
    }

    void BufMgr::disposePage(File *file, const PageId pageNo) {
        FrameId frameId;
        try {
            hashTable->lookup(file, pageNo, frameId);
        } catch (const HashNotFoundException &e) {
            return;
        }
        BufDesc &bufferDesc = bufDescTable[frameId];
        if (bufferDesc.valid) {
            hashTable->remove(file, pageNo);
            bufferDesc.Clear();
            file->deletePage(pageNo);
        }
    }

    void BufMgr::printSelf(void) {
        BufDesc *tmpBuffer;
        int validFrames = 0;

        for (std::uint32_t i = 0; i < numBufs; i++) {
            tmpBuffer = &(bufDescTable[i]);
            std::cout << "FrameNo:" << i << " ";
            tmpBuffer->Print();

            if (tmpBuffer->valid == true)
                validFrames++;
        }

        std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
    }

}
