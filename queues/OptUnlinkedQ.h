#pragma once

#ifndef OPT_UNLINKED_Q_H_
#define OPT_UNLINKED_Q_H_

#include <atomic>
#include <set>

#include <ssmem.h>

#include "utilities.h"

template<class T> class OptUnlinkedQ {
private:
    class PersistentNode {
    public:
        T item;
        uint64_t index;
        bool linked;

        void initialize(T value) {
            item = value;
            linked = false;

            // verify linked is set to false before index is later increased
            std::atomic_thread_fence(std::memory_order_release);
        }

        void initialize() {
            initialize(T());
        }
    } __attribute__((aligned (32)));

    class VolatileNode {
    public:
        T item;
        uint64_t index;
        std::atomic<VolatileNode*> next;
        PersistentNode* persistentNode;

        void initialize(T value) {
            item = value;
            next = nullptr;
            persistentNode = static_cast<PersistentNode*>(ssmem_alloc(alloc, sizeof(PersistentNode)));
            persistentNode->initialize(value);
        }

        void initialize() {
            initialize(T());
        }
    } __attribute__((aligned (32)));

    VolatileNode* allocVolatileNode() {
        void* volatileNode = ssmem_alloc(volatileAlloc, sizeof(VolatileNode));
        return static_cast<VolatileNode*>(volatileNode);
    }

public:
    OptUnlinkedQ() :
        Head(allocVolatileNode()),
        Tail(Head.load())
    {
        Head.load()->initialize();
        Head.load()->index = 0;
        Head.load()->persistentNode->index = 0;
        
        initializeNodeToRetire();

        for (int i = 0; i < MAX_THREADS; i++) {
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
                newNode->persistentNode->index = tail->index + 1;
                newNode->index = newNode->persistentNode->index;
                if (tail->next.compare_exchange_strong(tailNext, newNode)) {
                    newNode->persistentNode->linked = true;
                    FLUSH(newNode->persistentNode);
                    Tail.compare_exchange_strong(tail, newNode);
                    break;
                }
            }
            Tail.compare_exchange_strong(tail, tailNext);
        }
    }

    void recover() {
        initializeNodeToRetire();
        
        uint64_t headIndex = getMaxLocalHeadIndex();
        
        std::set<PersistentNode*, decltype(nodeCmp)*> queueNodes(nodeCmp); // Not including the new dummy PersistentNode we will later allocate
        getQueueNodesAndRetireOthers(headIndex, queueNodes); // retiring alloc's nodes; volatileAlloc is assumed to be reset
        
        // We allocate a new dummy PersistentNode only after retiring non-queue PersistenNode objects, for preventing retiring the dummy node
        recoverHead(headIndex);

        recoverVolatileQueue(queueNodes);
    }

private:
    std::atomic<VolatileNode*> Head DOUBLE_CACHE_LINE_ALIGNED;
    std::atomic<VolatileNode*> Tail DOUBLE_CACHE_LINE_ALIGNED;
    
    struct LocalData {
        VolatileNode* nodeToRetire CACHE_LINE_ALIGNED;
        uint64_t headIndex CACHE_LINE_ALIGNED;
    } DOUBLE_CACHE_LINE_ALIGNED;

    LocalData localData[MAX_THREADS];

    void initializeNodeToRetire() {
        for (int i = 0; i < MAX_THREADS; i++) {
            localData[i].nodeToRetire = nullptr;
        }
    }

    uint64_t getMaxLocalHeadIndex() {
        uint64_t headIndex = 0;
        for (int i = 0; i < MAX_THREADS; i++) {
            if (localData[i].headIndex > headIndex)
                headIndex = localData[i].headIndex;
        }
        
        return headIndex;
    }

    static bool nodeCmp(PersistentNode* node1, PersistentNode* node2) { 
        return node1->index < node2->index; 
    }

    void getQueueNodesAndRetireOthers(uint64_t headIndex, 
        std::set<PersistentNode*, decltype(nodeCmp)*>& queueNodes) {        
        for (auto curr = alloc->mem_chunks; curr != nullptr; curr = curr->next) {
            PersistentNode* currChunk = static_cast<PersistentNode*>(curr->obj);
            uint64_t numOfNodes = SSMEM_DEFAULT_MEM_SIZE / sizeof(PersistentNode);
            for (uint64_t i = 0; i < numOfNodes; i++) {
                PersistentNode* currNode = currChunk + i;
                if (currNode->linked && currNode->index > headIndex) {
                    queueNodes.insert(currNode);
                }
                else {
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

    void recoverVolatileQueue(std::set<PersistentNode*, decltype(nodeCmp)*>& queueNodes) {
        VolatileNode* predNode = Head.load();
        for (auto persistentNode : queueNodes) {
            VolatileNode* node = allocVolatileNode();
            predNode->next.store(node);
            node->item = persistentNode->item;
            node->index = persistentNode->index;
            node->persistentNode = persistentNode;

            predNode = node;
        }
        VolatileNode* lastNode = predNode;
        lastNode->next.store(nullptr);

        Tail.store(lastNode);
    }
};

#endif /* OPT_UNLINKED_Q_H_ */

