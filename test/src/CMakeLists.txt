add_subdirectory(bgw)
add_subdirectory(net)
add_subdirectory(telemetry)
target_include_directories(${PROJECT_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/test/src)
