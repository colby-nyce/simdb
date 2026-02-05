// <AppRegistration.hpp> -*- C++ -*-

#pragma once

#include "simdb/apps/AppManager.hpp"

template <typename AppT> struct AppRegistration
{
    AppRegistration() { simdb::AppManager::registerApp<AppT>(); }
};

#define SIMDB_CONCATENATE_DETAIL(x, y) x##y
#define SIMDB_CONCATENATE(x, y) SIMDB_CONCATENATE_DETAIL(x, y)

/// Use this macro in any translation unit to register an application with
/// SimDB. This should typically be done at file scope to ensure your app
/// factory is registered before much else has happened.
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
    AppRegistration<ApplicationType> SIMDB_CONCATENATE(__simdb_app_registration_, __COUNTER__)
