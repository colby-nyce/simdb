// <QueueRepo.hpp> -*- C++ -*-

#pragma once

#include "simdb/pipeline/Queue.hpp"
#include "simdb/Exceptions.hpp"

/// The classes in this file are used to create all the required
/// simdb::ConcurrentQueue(s) needed for all apps' pipeline stages.

namespace simdb::pipeline {

class QueuePlaceholder
{
public:
    virtual ~QueuePlaceholder() = default;
    virtual std::unique_ptr<QueueBase> createQueue() = 0;
    virtual void assignQueue(QueueBase* queue) = 0;
    virtual bool hasQueue() const = 0;
};

template <typename T>
class InputQueuePlaceholder : public QueuePlaceholder
{
public:
    /// Create placeholder with a backpointer to the stage's queue member variable.
    /// We will assign the queue to the stage variable when the queue is created.
    InputQueuePlaceholder(ConcurrentQueue<T>*& queue)
        : queue_(queue)
    {
        if (queue_)
        {
            throw DBException("Input queue placeholder cannot be initialized with non-null queue pointer.");
        }
    }

    std::unique_ptr<QueueBase> createQueue() override
    {
        auto queue = std::make_unique<Queue<T>>();
        queue_ = &queue->get();
        return queue;
    }

    void assignQueue(QueueBase* queue) override
    {
        auto typed_queue = dynamic_cast<Queue<T>*>(queue);
        if (!typed_queue)
        {
            throw DBException("Incompatible queue types in binding");
        }
        queue_ = &typed_queue->get();
    }

    bool hasQueue() const override
    {
        return queue_ != nullptr;
    }

    ConcurrentQueue<T>* getQueue()
    {
        return queue_;
    }

private:
    ConcurrentQueue<T>*& queue_;
};

template <typename T>
class OutputQueuePlaceholder : public QueuePlaceholder
{
public:
    /// Create placeholder with a backpointer to the stage's queue member variable.
    /// We will assign the queue to the stage variable when the queue is created.
    OutputQueuePlaceholder(ConcurrentQueue<T>*& queue)
        : queue_(queue)
    {
        if (queue_)
        {
            throw DBException("Output queue placeholder cannot be initialized with non-null queue pointer.");
        }
    }

    std::unique_ptr<QueueBase> createQueue() override
    {
        auto queue = std::make_unique<Queue<T>>();
        queue_ = &queue->get();
        return queue;
    }

    void assignQueue(QueueBase* queue) override
    {
        auto typed_queue = dynamic_cast<Queue<T>*>(queue);
        if (!typed_queue)
        {
            throw DBException("Incompatible queue types in binding");
        }
        queue_ = &typed_queue->get();
    }

    bool hasQueue() const override
    {
        return queue_ != nullptr;
    }

    ConcurrentQueue<T>* getQueue()
    {
        return queue_;
    }

private:
    ConcurrentQueue<T>*& queue_;
};

class QueueRepo
{
public:
    template <typename T>
    void addInPortPlaceholder(const std::string& stage_name,
                              const std::string& port_name,
                              ConcurrentQueue<T>*& queue)
    {
        if (finalized_)
        {
            throw DBException("Cannot add input port placeholder; QueueRepo already finalized.");
        }

        std::string key = stage_name + "." + port_name;
        auto& placeholder = input_placeholders_[key];
        if (placeholder) {
            throw DBException("Input port placeholder '" + key + "' already exists in QueueRepo");
        }
        placeholder = std::make_unique<InputQueuePlaceholder<T>>(queue);
    }

    template <typename T>
    void addOutPortPlaceholder(const std::string& stage_name,
                               const std::string& port_name,
                               ConcurrentQueue<T>*& queue)
    {
        if (finalized_)
        {
            throw DBException("Cannot add output port placeholder; QueueRepo already finalized.");
        }

        std::string key = stage_name + "." + port_name;
        auto& placeholder = output_placeholders_[key];
        if (placeholder) {
            throw DBException("Output port placeholder '" + key + "' already exists in QueueRepo");
        }
        placeholder = std::make_unique<OutputQueuePlaceholder<T>>(queue);
    }

    void bind(const std::string& output_port_full_name,
              const std::string& input_port_full_name)
    {
        if (finalized_)
        {
            throw DBException("Cannot bind ports; QueueRepo already finalized.");
        }
        port_bindings_[output_port_full_name] = input_port_full_name;
    }

    void finalizeBindings()
    {
        for (const auto& [out_port, in_port] : port_bindings_)
        {
            auto out_it = output_placeholders_.find(out_port);
            if (out_it == output_placeholders_.end())
            {
                throw DBException("Output port placeholder '" + out_port + "' not found in QueueRepo");
            }
            auto in_it = input_placeholders_.find(in_port);
            if (in_it == input_placeholders_.end())
            {
                throw DBException("Input port placeholder '" + in_port + "' not found in QueueRepo");
            }

            auto queue = out_it->second->createQueue();
            queues_.emplace_back(std::move(queue));
            in_it->second->assignQueue(queues_.back().get());
        }

        for (const auto& [key, placeholder] : input_placeholders_)
        {
            if (!placeholder->hasQueue())
            {
                auto queue = placeholder->createQueue();
                queues_.emplace_back(std::move(queue));
                unbound_input_queues_.insert(key);
            }
        }

        for (const auto& [key, placeholder] : output_placeholders_)
        {
            if (!placeholder->hasQueue())
            {
                auto queue = placeholder->createQueue();
                queues_.emplace_back(std::move(queue));
                unbound_output_queues_.insert(key);
            }
        }

        finalized_ = true;
    }

    template <typename T>
    ConcurrentQueue<T>* getInPortQueue(const std::string& port_full_name)
    {
        auto it = input_placeholders_.find(port_full_name);
        if (it == input_placeholders_.end())
        {
            throw DBException("Input port placeholder '" + port_full_name + "' not found in QueueRepo");
        }
        auto typed_placeholder = dynamic_cast<InputQueuePlaceholder<T>*>(it->second.get());
        if (!typed_placeholder)
        {
            throw DBException("Incompatible queue types for input port '" + port_full_name + "'");
        }
        unbound_input_queues_.erase(port_full_name);
        return typed_placeholder->getQueue();
    }

    template <typename T>
    ConcurrentQueue<T>* getOutPortQueue(const std::string& port_full_name)
    {
        auto it = output_placeholders_.find(port_full_name);
        if (it == output_placeholders_.end())
        {
            throw DBException("Output port placeholder '" + port_full_name + "' not found in QueueRepo");
        }
        auto typed_placeholder = dynamic_cast<OutputQueuePlaceholder<T>*>(it->second.get());
        if (!typed_placeholder)
        {
            throw DBException("Incompatible queue types for output port '" + port_full_name + "'");
        }
        unbound_output_queues_.erase(port_full_name);
        return typed_placeholder->getQueue();
    }

    void validateQueues()
    {
        std::ostringstream oss;

        if (!unbound_input_queues_.empty())
        {
            oss << "The following input queues are not attached to anything:\n";
            for (const auto& name : unbound_input_queues_)
            {
                oss << "    " << name << "\n";
            }
        }

        if (!unbound_output_queues_.empty())
        {
            oss << "The following output queues are not attached to anything:\n";
            for (const auto& name : unbound_output_queues_)
            {
                oss << "    " << name << "\n";
            }
        }

        auto err = oss.str();
        if (!err.empty())
        {
            throw DBException(err);
        }
    }

private:
    std::unordered_map<std::string, std::unique_ptr<QueuePlaceholder>> input_placeholders_;
    std::unordered_map<std::string, std::unique_ptr<QueuePlaceholder>> output_placeholders_;
    std::unordered_map<std::string, std::string> port_bindings_;
    std::set<std::string> unbound_input_queues_;
    std::set<std::string> unbound_output_queues_;
    std::vector<std::unique_ptr<QueueBase>> queues_;
    bool finalized_ = false;
};

} // namespace simdb::pipeline
