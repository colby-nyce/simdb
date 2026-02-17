#include "simdb/apps/AppManager.hpp"

namespace simdb {
std::map<std::string, size_t>& AppManager::getEnabledApps_()
{
    static std::map<std::string, size_t> enabled_apps;
    return enabled_apps;
}
} // namespace simdb
