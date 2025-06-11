#pragma once

#include "simdb/apps/AppManager.hpp"

template <typename AppT>
struct AppRegistration
{
    AppRegistration()
    {
        simdb::AppManager::getInstance().registerApp<AppT>();
    }
};

/// Use this macro in any translation unit to register an application with SimDB.
/// This should typically be done at file scope to ensure your app factory is
/// registered before much else has happened.
///
///   MyApp.hpp:
///   ----------------------------------------
///   class MyApp : public simdb::App
///   { ... };
///
///   MyApp.cpp:
///   ----------------------------------------
///   REGISTER_SIMDB_APPLICATION(MyApp);
///
#define REGISTER_SIMDB_APPLICATION(ApplicationType) \
    AppRegistration<ApplicationType> __simdb_app_registration_##_COUNTER__
