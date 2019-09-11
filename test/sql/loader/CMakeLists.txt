if (CMAKE_BUILD_TYPE MATCHES Debug)
install(
  FILES timescaledb--mock-1.sql
        timescaledb--mock-2.sql
        timescaledb--mock-3.sql
        timescaledb--mock-4.sql
        timescaledb--mock-5.sql
        timescaledb--mock-6.sql
        timescaledb--mock-broken.sql
        timescaledb--mock-1--mock-2.sql
        timescaledb--mock-2--mock-3.sql
        timescaledb--mock-3--mock-4.sql
        timescaledb--mock-5--mock-6.sql
        timescaledb--mock-broken--mock-5.sql
        timescaledb--0.0.0.sql
  DESTINATION "${PG_SHAREDIR}/extension")
endif (CMAKE_BUILD_TYPE MATCHES Debug)
