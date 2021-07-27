#pragma once

#ifndef UNLINKED_Q_H_
#define UNLINKED_Q_H_

#include <atomic>
#include <set>
#include <assert.h>

#include <ssmem.h>

#include "utilities.h"

template<class T> class UnlinkedQ {
private:
    class Node {
    public:
        T item;
        std::atomic<Node*> next;
        bool linked;
        uint64_t index;

        void initialize(T value) {
            item = value;
            next = nullptr;
            linked = false;

            // verify linked is set to false before index is later increased
            std::atomic_thread_fence(std::memory_order_release);
        }

        void initialize() {
            initialize(T());
        }
    } __attribute__((aligned (32)));

    Node* allocNode() {
        void* node = ssmem_alloc(alloc, sizeof(Node));
        return static_cast<Node*>(node);
    }

    struct PointerAndIndex {
    public:
        uint64_t index;
        Node* ptr;

        PointerAndIndex(Node* p, uint64_t i) : ptr(p), index(i) {}
    };

    /*
    GCC attribute that is run once (when the library is loaded), in order to
    make sure that the std::atomic<PointerAndIndex> implementation is lock free.
    */
    __attribute__((constructor))
    static void VerifyLockFreedom(void)
    {
        assert(std::atomic<PointerAndIndex>().is_lock_free());
    }

public:
    UnlinkedQ() :
        Head(PointerAndIndex(allocNode(), 0)),
        Tail(Head.load().ptr)
    {
        Node* head = Head.load().ptr;
        head->initialize();
        head->index = 0;
        FLUSH(&Head);
        SFENCE();
        
        initializeNodeToRetire();
    }

    bool deq(T* dequeuedItem, int threadId) {
        while (true) {
            PointerAndIndex head = Head.load();
            Node* headNext = head.ptr->next.load();
            if (headNext == nullptr) {
                FLUSH(&Head);
                SFENCE();
                return false;
            }
            
            if (Head.compare_exchange_strong(head, PointerAndIndex(headNext, headNext->index))) {
                *dequeuedItem = headNext->item;
                FLUSH(&Head);
                SFENCE();

                if (nodeToRetire[threadId].ptr) { // It equals NULL in the first successful deq
                    ssmem_free(alloc, nodeToRetire[threadId].ptr);
                }
                nodeToRetire[threadId].ptr = head.ptr;
                
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
                newNode->index = tail->index + 1;
                if (tail->next.compare_exchange_strong(tailNext, newNode)) {
                    newNode->linked = true;
                    FLUSH(newNode);
                    Tail.compare_exchange_strong(tail, newNode);
                    break;
                }
            }
            Tail.compare_exchange_strong(tail, tailNext);
        }
    }

    void recover() {
        initializeNodeToRetire();

        std::set<Node*, decltype(nodeCmp)*> queueNodes(nodeCmp); // Not including the new dummy node we will later allocate
        getQueueNodesAndRetireOthers(queueNodes);

        // We allocate a new dummy node only after retiring non-queue nodes, for preventing retiring the dummy node
        recoverHead();

        recoverLinksAndTail(queueNodes);
    }

private:
    std::atomic<PointerAndIndex> Head DOUBLE_CACHE_LINE_ALIGNED;
    std::atomic<Node*> Tail DOUBLE_CACHE_LINE_ALIGNED;
    
    struct NodePtr {
        Node* ptr;
    } DOUBLE_CACHE_LINE_ALIGNED;

    NodePtr nodeToRetire[MAX_THREADS];

    void initializeNodeToRetire() {
        for (int i = 0; i < MAX_THREADS; i++) {
            nodeToRetire[i].ptr = nullptr;
        }
    }

    static bool nodeCmp(Node* node1, Node* node2) { 
        return node1->index < node2->index; 
    }

    void getQueueNodesAndRetireOthers(std::set<Node*, decltype(nodeCmp)*>& queueNodes) {
        for (auto curr = alloc->mem_chunks; curr != nullptr; curr = curr->next) {
            Node* currChunk = static_cast<Node*>(curr->obj);
            uint64_t numOfNodes = SSMEM_DEFAULT_MEM_SIZE / sizeof(Node);
            for (uint64_t i = 0; i < numOfNodes; i++) {
                Node* currNode = currChunk + i;
                if (currNode->linked && currNode->index > Head.load().index) {
                    queueNodes.insert(currNode);
                }
                else {
                    ssmem_free(alloc, currNode);
                }
            }
        }
    }

    void recoverHead() {
        uint64_t headIndex = Head.load().index;
        Node* head = allocNode();
        head->index = headIndex;
        Head.store(PointerAndIndex(head, headIndex));
    }

    void recoverLinksAndTail(std::set<Node*, decltype(nodeCmp)*>& queueNodes) {
        Node* predNode = Head.load().ptr;
        for (auto node : queueNodes) {
            predNode->next.store(node);
            predNode = node;
        }
        Node* lastNode = predNode;
        lastNode->next.store(nullptr);

        Tail.store(lastNode);
    }
};

#endif /* UNLINKED_Q_H_ */

