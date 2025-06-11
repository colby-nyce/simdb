/// SimDB pipelines operate on a "chain" of functions that process data
/// entries. Each function can modify the entry (e.g. compress, transform, etc)
/// before continuing to the next function in the chain.

#pragma once

#include <utility>
#include <cstddef>

namespace simdb
{

class PipelineEntryBase;
using PipelineFunc = void(*)(PipelineEntryBase&);

/// Node in a pipeline chain (linked list).
class PipelineChainLink
{
public:
    PipelineChainLink(PipelineFunc func = nullptr)
        : func_(func)
    {
    }

    ~PipelineChainLink()
    {
        if (next_)
        {
            //delete next_;
        }
    }

    void setNext(PipelineFunc next_func)
    {
        if (next_)
        {
            //delete next_;
        }
        next_ = new PipelineChainLink(next_func);
    }

    PipelineChainLink* getNext() const
    {
        return next_;
    }

    PipelineChainLink* getTail()
    {
        PipelineChainLink* current = this;
        while (current->next_)
        {
            current = current->next_;
        }
        return current;
    }

    PipelineFunc getFunc() const
    {
        return func_;
    }

    operator bool() const
    {
        return func_ != nullptr;
    }

    void operator()(PipelineEntryBase& entry) const
    {
        if (func_)
        {
            func_(entry);
        }
    }

    PipelineChainLink* clone() const
    {
        PipelineChainLink* new_link = new PipelineChainLink(func_);
        if (next_)
        {
            new_link->next_ = next_->clone();
        }
        return new_link;
    }

private:
    PipelineFunc func_;
    PipelineChainLink* next_ = nullptr;
    friend class PipelineChain;
};

/// A chain of pipeline functions that can be composed together.
/// Similar to std::list<PipelineFunc> but with a more structured
/// interface for adding functions and managing the chain.
class PipelineChain
{
public:
    PipelineChain(PipelineFunc head = nullptr)
        : head_(head ? new PipelineChainLink(head) : nullptr)
    {
    }

    ~PipelineChain()
    {
        reset();
    }

    void setHead(PipelineFunc head)
    {
        reset();
        head_ = new PipelineChainLink(head);
    }

    PipelineChain operator+(PipelineFunc func) const
    {
        PipelineChain new_chain;
        if (head_)
        {
            new_chain.head_ = head_->clone();
            PipelineChainLink* tail = new_chain.head_->getTail();
            tail->setNext(func);
        }
        else
        {
            new_chain.setHead(func);
        }
        return new_chain;
    }

    PipelineChain operator+(const PipelineChain& other) const
    {
        PipelineChain new_chain;
        if (head_)
        {
            new_chain.head_ = head_->clone();
        }
        else
        {
            return other;
        }

        if (other.head_)
        {
            PipelineChainLink* tail = new_chain.head_->getTail();
            tail->setNext(other.head_->getFunc());
            PipelineChainLink* current = other.head_->getNext();
            while (current)
            {
                tail->setNext(current->getFunc());
                tail = tail->getNext();
                current = current->getNext();
            }
        }
        return new_chain;
    }

    PipelineChain& operator+=(PipelineFunc func)
    {
        if (head_)
        {
            head_->getTail()->setNext(func);
        }
        else
        {
            setHead(func);
        }
        return *this;
    }

    PipelineChain& operator+=(const PipelineChain& other)
    {
        if (other.head_)
        {
            if (head_)
            {
                head_->getTail()->setNext(other.head_->getFunc());
                PipelineChainLink* tail = head_->getTail();
                PipelineChainLink* current = other.head_->getNext();
                while (current)
                {
                    tail->setNext(current->getFunc());
                    tail = tail->getNext();
                    current = current->getNext();
                }
            }
            else
            {
                head_ = other.head_->clone();
            }
        }
        return *this;
    }

    operator bool() const
    {
        return head_ != nullptr;
    }

    void reverse()
    {
        if (!head_)
        {
            return;
        }

        PipelineChainLink* prev = nullptr;
        PipelineChainLink* current = head_;
        PipelineChainLink* next = nullptr;

        while (current)
        {
            next = current->next_;
            current->next_ = prev;
            prev = current;
            current = next;
        }

        head_ = prev;
    }

    void reset()
    {
        if (head_)
        {
            //delete head_;
            head_ = nullptr;
        }
    }

    // Process the entry through the pipeline chain.
    void operator()(PipelineEntryBase& entry) const
    {
        if (head_)
        {
            PipelineChainLink* current = head_;
            while (current)
            {
                (*current)(entry);
                current = current->getNext();
            }
        }
    }

private:
    PipelineChainLink* head_ = nullptr;
};

} // namespace simdb
