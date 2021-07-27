#pragma once

#ifndef OPT_LINKED_Q_H_
#define OPT_LINKED_Q_H_

#include <atomic>
#include <set>

#include <ssmem.h>

#include "utilities.h"

template<class T> class OptLinkedQ {
private:
    class VolatileNode;

    class PersistentNode {
    public:
        T item;
        PersistentNode* pred;
        uint64_t index;

        void initialize(T value) {
            item = value;
        }

        void initialize() {
            initialize(T());
        }
    } __attribute__((aligned (32)));

    class VolatileNode {
    public:
        T item;
        std::atomic<VolatileNode*> next;
        std::atomic<VolatileNode*> pred;
        uint64_t index;
        PersistentNode* persistentNode;

        void initialize(T value) {
            item = value;
            next.store(nullptr, std::memory_order_relaxed);
            persistentNode = static_cast<PersistentNode*>(ssmem_alloc(alloc, sizeof(PersistentNode)));
            persistentNode->initialize(value);
        }

        void initialize() {
            initialize(T());
        }
    } __attribute__((aligned (32)));

    static const int ValidBitPositionInPointer = 0;
    static const int ValidBitPositionInIndex = sizeof(uint64_t) * 8 - 1;

    VolatileNode* allocVolatileNode() {
        void* volatileNode = ssmem_alloc(volatileAlloc, sizeof(VolatileNode));
        return static_cast<VolatileNode*>(volatileNode);
    }

public:
    OptLinkedQ() :
        Head(allocVolatileNode()),
        Tail(Head.load())
    {
        VolatileNode* dummyNode = Head.load();

        dummyNode->initialize();
        dummyNode->pred.store(nullptr, std::memory_order_relaxed);
        // No need to persist the dummy node, recovery will anyhow not reach it

        for (int i = 0; i < MAX_THREADS; i++) {
            localData[i].nodeToRetire = nullptr;

            resetLastEnqueueForThread(i);

            __writeq(0, &(localData[i].headIndex));
        }
        SFENCE();
    }

    bool deq(T* dequeuedItem, int threadId) {
        while (true) {
            VolatileNode* head = Head.load();
            VolatileNode* headNext = head->next.load();
            if (headNext == nullptr) {
                __writeq(head->index, &(localData[threadId].headIndex));
                SFENCE();
                return false;
            }
           
            if (Head.compare_exchange_strong(head, headNext)) {
                *dequeuedItem = headNext->item;
                __writeq(headNext->index, &(localData[threadId].headIndex));
                SFENCE();

                headNext->pred.store(nullptr, std::memory_order_relaxed);

                if (localData[threadId].nodeToRetire) { // It equals NULL in the first successful deq
                    ssmem_free(alloc, localData[threadId].nodeToRetire->persistentNode);
                    ssmem_free(volatileAlloc, localData[threadId].nodeToRetire);
                }
                localData[threadId].nodeToRetire = head;
               
                return true;
            }
        }
    }

    void enq(T item, int threadId) {
        VolatileNode* newNode = allocVolatileNode();
        newNode->initialize(item);
        while (true) {
            VolatileNode* tail = Tail.load();
            VolatileNode* tailNext = tail->next.load();
            if (tailNext == nullptr) {
                newNode->pred.store(tail, std::memory_order_relaxed);
                newNode->index = tail->index + 1;
                newNode->persistentNode->pred = tail->persistentNode;
                std::atomic_thread_fence(std::memory_order_release); // pred of newNode->persistentNode must be written before its index
                newNode->persistentNode->index = newNode->index;
                if (tail->next.compare_exchange_strong(tailNext, newNode)) {
                    Tail.compare_exchange_strong(tail, newNode);
                    flushNotPersistedSuffix(newNode);
                    recordLastEnqueue(newNode, threadId);
                    SFENCE();

                    newNode->pred.store(nullptr, std::memory_order_relaxed);
                    break;
                }
            }
            Tail.compare_exchange_strong(tail, tailNext);
        }
    }

    void recover() {
        initializeNodeToRetire();

        uint64_t headIndex = getMaxLocalHeadIndex();
        
        std::set<LastEnqueue, decltype(lastEnqueueCmp)*> potentialTails(lastEnqueueCmp);
        getPotentialTails(potentialTails, headIndex);

        std::set<PersistentNode*> queueNodes; // Not including the new dummy PersistentNode we will later allocate
        getQueueNodes(potentialTails, queueNodes, headIndex);
        
        retireNonQueueNodes(queueNodes, headIndex); // retiring alloc's nodes; volatileAlloc is assumed to be reset

        // We allocate a new dummy PersistentNode only after retiring non-queue PersistenNode objects, for preventing retiring the dummy node
        recoverHead(headIndex);

        recoverVolatileQueue(queueNodes);

        recoverLastEnqueues();
       
        SFENCE();
    }

private:
    std::atomic<VolatileNode*> Head DOUBLE_CACHE_LINE_ALIGNED;
    std::atomic<VolatileNode*> Tail DOUBLE_CACHE_LINE_ALIGNED;

    struct LastEnqueue {
        PersistentNode* ptr;
        uint64_t index;        
    };

    struct LocalData {
        VolatileNode* nodeToRetire CACHE_LINE_ALIGNED;
        int validBit;
        int lastEnqueuesIndex;
        LastEnqueue lastEnqueues[2] CACHE_LINE_ALIGNED;
        uint64_t headIndex;
    } DOUBLE_CACHE_LINE_ALIGNED;

    LocalData localData[MAX_THREADS];

    void flushNotPersistedSuffix(VolatileNode* notPersisted) {
        while (true) {
            VolatileNode* pred = notPersisted->pred.load();
            if (pred == nullptr) {
                break;
            }
            FLUSH(notPersisted->persistentNode);
            notPersisted = pred;
        }
    }

    uint64_t zeroBit(uint64_t value, int bitIndex) {
        return value & ~(1UL << bitIndex);
    }

    uint64_t applyBit(uint64_t value, int bitIndex, uint64_t bitValue) {
        return zeroBit(value, bitIndex) | (bitValue << bitIndex);
    }

    uint64_t getBit(uint64_t value, int bitIndex) {
        return (value >> bitIndex) & 1UL;
    }

    void recordLastEnqueue(VolatileNode* newNode, int threadId) {
        int i = localData[threadId].lastEnqueuesIndex;

        // We use a validity bit to form an atomic write of the pointer and index field, because in a non-atomic write - 
        // if the index is written first, then the pointer might point to a reclaimed node
        // that another thread tried to enqueue and set its index to newNode->index
        __writeq((void*)applyBit((uint64_t)newNode->persistentNode, ValidBitPositionInPointer, localData[threadId].validBit), &(localData[threadId].lastEnqueues[i].ptr));
        __writeq(applyBit(newNode->index, ValidBitPositionInIndex, localData[threadId].validBit), &(localData[threadId].lastEnqueues[i].index));

        localData[threadId].validBit ^= i; // flip validBit if i=1
        localData[threadId].lastEnqueuesIndex ^= 1; // == (i + 1) % 2. Namely, flip index on each enqueue
    }

    void initializeNodeToRetire() {
        for (int i = 0; i < MAX_THREADS; i++) {
            localData[i].nodeToRetire = nullptr;
        }
    }

    void resetLastEnqueueForThread(int threadId) {
        __writeq(0, &(localData[threadId].lastEnqueues[0].index));
        __writeq(0, &(localData[threadId].lastEnqueues[1].index));
        __writeq(0, &(localData[threadId].lastEnqueues[0].ptr));
        __writeq(0, &(localData[threadId].lastEnqueues[1].ptr));
        localData[threadId].validBit = 1;
        localData[threadId].lastEnqueuesIndex = 0;
    }
        
    uint64_t getMaxLocalHeadIndex() {
        uint64_t headIndex = 0;
        for (int i = 0; i < MAX_THREADS; i++) {
            if (localData[i].headIndex > headIndex)
                headIndex = localData[i].headIndex;
        }

        return headIndex;
    }

    static bool lastEnqueueCmp(const LastEnqueue& potentialTail1, const LastEnqueue& potentialTail2) {
        return potentialTail1.index < potentialTail2.index; 
    }
        
    void getPotentialTails(std::set<LastEnqueue, decltype(lastEnqueueCmp)*>& potentialTails, 
        uint64_t headIndex) {
        for (int i = 0; i < MAX_THREADS; i++) {
            for (int j = 0; j < 2; j++) {
                if (getBit(localData[i].lastEnqueues[j].index, ValidBitPositionInIndex) !=
                    getBit((uint64_t)localData[i].lastEnqueues[j].ptr, ValidBitPositionInPointer)) {
                    continue;
                }
                LastEnqueue potentialTail = localData[i].lastEnqueues[j];
                potentialTail.index = zeroBit(potentialTail.index, ValidBitPositionInIndex);
                potentialTail.ptr = (PersistentNode*)zeroBit((uint64_t)potentialTail.ptr, ValidBitPositionInPointer);
                if ((potentialTail.index <= headIndex) || !potentialTail.ptr) {
                    continue;
                }
                potentialTails.insert(potentialTail); // Add this valid potential tail
            }
        }
    }

    bool getQueueNodesIfTail(const LastEnqueue& potentialTail,
        std::set<PersistentNode*>& queueNodes, 
        uint64_t headIndex) {
        if (potentialTail.ptr->index != potentialTail.index) {
            return false;
        }

        PersistentNode* currNode = potentialTail.ptr;
        while (true) {
            queueNodes.insert(currNode);
            if (currNode->index == headIndex + 1) {
                return true;
            }
            PersistentNode* predNode = currNode->pred;
            if (predNode->index != currNode->index - 1) {
                queueNodes.clear();
                return false;
            }
            currNode = predNode;
        }
    }
 
    void getQueueNodes(
        const std::set<LastEnqueue, decltype(lastEnqueueCmp)*>& potentialTails,
        std::set<PersistentNode*>& queueNodes,
        uint64_t headIndex) {
        for (auto reversedIterator = potentialTails.rbegin(); 
            reversedIterator != potentialTails.rend();
            reversedIterator++) {
            if (getQueueNodesIfTail(*reversedIterator, queueNodes, headIndex)) {
                break;
            }
        }
    }

    void retireNonQueueNodes(const std::set<PersistentNode*>& queueNodes, uint64_t headIndex) {
        for (auto curr = alloc->mem_chunks; curr != nullptr; curr = curr->next) {
            PersistentNode* currChunk = static_cast<PersistentNode*>(curr->obj);
            uint64_t numOfNodes = SSMEM_DEFAULT_MEM_SIZE / sizeof(PersistentNode);
            for (uint64_t i = 0; i < numOfNodes; i++) {
                PersistentNode* currNode = currChunk + i;
                if (queueNodes.find(currNode) == queueNodes.end()) {
                    if (currNode->index > headIndex) {
                        currNode->index = 0;
                        FLUSH(currNode);
                    }
                    ssmem_free(alloc, currNode);
                }
            }
        }
    }

    void recoverHead(uint64_t headIndex) {
        VolatileNode* head = allocVolatileNode();
        head->persistentNode = static_cast<PersistentNode*>(ssmem_alloc(alloc, sizeof(PersistentNode)));
        head->index = headIndex;
        head->persistentNode->index = headIndex;
        Head.store(head);
    }

    void setPersistedSuffixAndRecoverTail(VolatileNode* volatileTail) {
        volatileTail->pred.store(nullptr, std::memory_order_relaxed);
        Tail.store(volatileTail);
    }
    
    void recoverVolatileQueue(const std::set<PersistentNode*>& queueNodes) {
        VolatileNode* volatileTail = nullptr;
        VolatileNode* subsequentVolatileNode = nullptr;

        if (queueNodes.size() == 0) { 
            // The queue is empty
            volatileTail = Head.load();
        } else {   
            for (auto reversedIterator = queueNodes.rbegin(); 
                reversedIterator != queueNodes.rend();
                reversedIterator++) {
                PersistentNode* persistentNode = *reversedIterator;

                VolatileNode* volatileNode = allocVolatileNode();
                volatileNode->next.store(subsequentVolatileNode);
                volatileNode->item = persistentNode->item;
                volatileNode->index = persistentNode->index;
                volatileNode->persistentNode = persistentNode;
                if (!volatileTail) {
                    // This is the first iteration - set the volatile tail
                    volatileTail = volatileNode;
                }

                subsequentVolatileNode = volatileNode;
            }
        }

        Head.load()->next.store(subsequentVolatileNode);

        // No need to set the pred field for any node but the last one
        setPersistedSuffixAndRecoverTail(volatileTail);
    }

    bool isValidTail(const LastEnqueue& potentialTail) {
        return (zeroBit(potentialTail.index, ValidBitPositionInIndex) == Tail.load()->index) &&
            ((PersistentNode*)zeroBit((uint64_t)potentialTail.ptr, ValidBitPositionInPointer) == Tail.load()->persistentNode) &&
            (zeroBit(potentialTail.index, ValidBitPositionInIndex) > Head.load()->index) &&
            (getBit(potentialTail.index, ValidBitPositionInIndex) == getBit((uint64_t)potentialTail.ptr, ValidBitPositionInPointer));
    }

    void recoverLastEnqueues() {
        for (int i = 0; i < MAX_THREADS; i++) {
            if (!isValidTail(localData[i].lastEnqueues[0]) && !isValidTail(localData[i].lastEnqueues[1])) {
                // Reset both last enqueue cells of thread i as they do not refer to the recovered tail
                resetLastEnqueueForThread(i);
            } else if (isValidTail(localData[i].lastEnqueues[0])) { // Thread i's first last enqueue cell refers to the recovered tail
                // We reset thread i's second cell,
                // set the second cell as the next one to be written,
                // and set the valid bit to the same one as the valid bit in the first cell 
                // (so that the next write to the first cell will be with the opposite valid bit value).
                __writeq(0, &(localData[i].lastEnqueues[1].index));
                __writeq(0, &(localData[i].lastEnqueues[1].ptr));
                localData[i].lastEnqueuesIndex = 1;
                localData[i].validBit = getBit(localData[i].lastEnqueues[0].index, ValidBitPositionInIndex);
            } else { // Thread i's second last enqueue cell refers to the recovered tail
                // We reset thread i's first cell,
                // set the first cell as the next one to be written,
                // and set the valid bit to the opposite one of the valid bit in the second cell
                // (so that the next write to the second cell will be with the opposite valid bit value).
                __writeq(0, &(localData[i].lastEnqueues[0].index));
                __writeq(0, &(localData[i].lastEnqueues[0].ptr));
                localData[i].lastEnqueuesIndex = 0;
                localData[i].validBit = getBit(localData[i].lastEnqueues[1].index, ValidBitPositionInIndex) ^ 1;
            }
        }
    }
};

#endif /* OPT_LINKED_Q_H_ */