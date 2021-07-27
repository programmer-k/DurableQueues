#pragma once

#ifndef LINKED_Q_H_
#define LINKED_Q_H_

#include <atomic>
#include <set>

#include <ssmem.h>

#include "utilities.h"

template<class T> class LinkedQ {
private:
    class Node {
    public:
        T item;
        std::atomic<Node*> next;
        std::atomic<Node*> pred;
        bool initialized;

        void initialize(T value) {
            // initialized is guaranteed to be false when node is allocated from pool
            item = value;
            next = nullptr;
            std::atomic_thread_fence(std::memory_order_release);
            initialized = true;
        }

        void initialize() {
            initialize(T());
        }
    } __attribute__((aligned (32)));

    Node* allocNode() {
        void* node = ssmem_alloc(alloc, sizeof(Node));
        return static_cast<Node*>(node);
    }

public:
    LinkedQ() :
        Head(allocNode()),
        Tail(Head.load())
    {
        Head.load()->initialize();
        Head.load()->pred.store(nullptr, std::memory_order_relaxed);
        FLUSH(Head);
        FLUSH(&Head);
        SFENCE();

        initializeNodeToPersistAndRetire();
    }

    bool deq(T* dequeuedItem, int threadId) {
        while (true) {
            Node* head = Head.load();
            Node* headNext = head->next.load();
            if (headNext == nullptr) {
                FLUSH(&Head);
                SFENCE();
                return false;
            }
            
            if (Head.compare_exchange_strong(head, headNext)) {
                *dequeuedItem = headNext->item;
                if (nodeToPersistAndRetire[threadId].ptr) { // It equals NULL in the first successful deq
                    FLUSH(&(nodeToPersistAndRetire[threadId].ptr->initialized));
                }
                FLUSH(&Head);
                SFENCE();

                headNext->pred.store(nullptr, std::memory_order_relaxed);

                if (nodeToPersistAndRetire[threadId].ptr) { // It equals NULL in the first successful deq
                    ssmem_free(alloc, nodeToPersistAndRetire[threadId].ptr);
                }
                head->initialized = false;
                nodeToPersistAndRetire[threadId].ptr = head;
                
                return true;
            }
        }
    }

    void enq(T item, int threadId) {
        Node* newNode = allocNode();
        newNode->initialize(item);
        while (true) {
            Node* tail = Tail.load();
            Node* tailNext = tail->next.load();
            if (tailNext == nullptr) {
                newNode->pred.store(tail, std::memory_order_relaxed);
                if (tail->next.compare_exchange_strong(tailNext, newNode)) {
                    flushNotPersistedSuffix(newNode);
                    Tail.compare_exchange_strong(tail, newNode);
                    newNode->pred.store(nullptr, std::memory_order_relaxed);
                    break;
                }
            }
            Tail.compare_exchange_strong(tail, tailNext);
        }
    }

    void recover() {
        initializeNodeToPersistAndRetire();

        std::set<Node*> queueNodes;
        bool didFlush = getQueueNodesIncludingDummy(queueNodes)
            || retireNonQueueNodes(queueNodes);

        Node* lastNode = *queueNodes.rbegin();
        setPersistedSuffixAndRecoverTail(lastNode);
        
        if (didFlush) {
            SFENCE();
        }
    }

private:
    std::atomic<Node*> Head DOUBLE_CACHE_LINE_ALIGNED;
    std::atomic<Node*> Tail DOUBLE_CACHE_LINE_ALIGNED;

    struct NodePtr {
        Node* ptr;
    } DOUBLE_CACHE_LINE_ALIGNED;

    NodePtr nodeToPersistAndRetire[MAX_THREADS];

    void initializeNodeToPersistAndRetire() {
        for (int i = 0; i < MAX_THREADS; i++) {
            nodeToPersistAndRetire[i].ptr = nullptr;
        }
    }

    void flushNotPersistedSuffix(Node* notPersisted) {
        do {
            FLUSH(notPersisted);
            notPersisted = notPersisted->pred.load();
        } while (notPersisted != nullptr);
    }

    bool getQueueNodesIncludingDummy(std::set<Node*>& queueNodes) {
        Node* currNode = Head.load();
    
        if (!currNode->initialized) {
            currNode->initialize();
            queueNodes.insert(currNode);
            return false;
        }
        
        while (true) {
            queueNodes.insert(currNode);
            Node* nextNode = currNode->next.load();
            if (nextNode == nullptr) {
                return false;
            }
            if (!nextNode->initialized) {
                currNode->next.store(nullptr, std::memory_order_relaxed);
                FLUSH(currNode);
                return true;
            }
            currNode = nextNode;
        }
    }

    bool retireNonQueueNodes(std::set<Node*>& queueNodes) {
        bool didFlush = false;

        for (auto curr = alloc->mem_chunks; curr != nullptr; curr = curr->next) {
            Node* currChunk = static_cast<Node*>(curr->obj);
            uint64_t numOfNodes = SSMEM_DEFAULT_MEM_SIZE / sizeof(Node);
            for (uint64_t i = 0; i < numOfNodes; i++) {
                Node* currNode = currChunk + i;
                if (queueNodes.find(currNode) == queueNodes.end()) {
                    if (currNode->initialized) {
                        currNode->initialized = false;
                        FLUSH(currNode);
                        didFlush = true;
                    }
                    ssmem_free(alloc, currNode);
                }
            }
        }

        return didFlush;
    }

    void setPersistedSuffixAndRecoverTail(Node* lastNode) {
        lastNode->pred.store(nullptr, std::memory_order_relaxed);
        Tail.store(lastNode);
    }

};

#endif /* LINKED_Q_H_ */

