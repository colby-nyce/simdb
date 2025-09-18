// <ConditionalLock.hpp> -*- C++ -*-

namespace simdb {

template <typename Mutex>
class ConditionalLock
{
public:
    ConditionalLock(Mutex& m, bool lock = true)
        : mutex_(m)
        , locked_(lock)
    {
        if (locked_)
        {
            mutex_.lock();
        }
    }

    ~ConditionalLock()
    {
        if (locked_)
        {
            mutex_.unlock();
        }
    }

private:
    Mutex& mutex_;
    const bool locked_;
};

} // namespace simdb
