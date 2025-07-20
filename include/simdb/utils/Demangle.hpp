#pragma once

#include <string>
#include <cxxabi.h>

namespace simdb {

/*!
 * \brief Represents the internal buffer size for demangling C++ symbols via
 * sparta::demangle
 */
#define DEMANGLE_BUF_LENGTH 4096

/*!
 * \brief Demangles a C++ symbol
 * \param Name Symbol name to demangle
 * \return Demangled name if successful. If failed, returns the input
 * name. Note that demangled names may match input name.
 * \note Demangling is limited by DEMANGLE_BUF_LENGTH. results may be
 * truncated or fail for very symbols. Change this value to support longer
 * symbol names.
 */
inline std::string demangle(const std::string& name) noexcept
{
    char buf[DEMANGLE_BUF_LENGTH];
    size_t buf_size = DEMANGLE_BUF_LENGTH;
    int status;
    char* out = __cxxabiv1::__cxa_demangle(name.c_str(), buf, &buf_size, &status);
    if (nullptr == out)
    {
        return name;
    }
    return std::string(out);
}

template <typename T>
inline std::string demangle_type() noexcept
{
    return demangle(typeid(T).name());
}

} // namespace simdb
