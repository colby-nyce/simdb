#pragma once

namespace simdb
{

/// \brief A simple RAII-style deferred lock that locks a mutex when constructed
/// and unlocks it when destructed.
template <typename MutexType>
class DeferredLock
{
public:
    DeferredLock(MutexType& mutex, bool lock_immediately = false)
        : mutex_(mutex)
    {
        if (lock_immediately)
        {
            mutex_.lock();
        }
    }

    ~DeferredLock()
    {
        mutex_.unlock();
    }

    void lock()
    {
        mutex_.lock();
    }

private:
    MutexType& mutex_;
};

} // namespace simdb
