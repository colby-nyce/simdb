// <DeferredLock.hpp> -*- C++ -*-

namespace simdb {

template <typename Mutex> class DeferredLock {
  public:
    DeferredLock(Mutex &m) : mutex_(m) {}
    ~DeferredLock() { mutex_.unlock(); }
    void lock() { mutex_.lock(); }

  private:
    Mutex &mutex_;
};

} // namespace simdb
