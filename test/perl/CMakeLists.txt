set(PERL_FILES AccessNode.pm DataNode.pm TimescaleNode.pm)

# Check if PostgreSQL was compiled with --enable-tap-tests
if(TAP_CHECKS AND EXISTS "${PG_PKGLIBDIR}/pgxs/src/test/perl")
  install(FILES ${PERL_FILES} DESTINATION "${PG_PKGLIBDIR}/pgxs/src/test/perl")
endif()
