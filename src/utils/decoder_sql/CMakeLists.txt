set(SOURCES decoder_sql.c)

add_library(decoder_sql MODULE ${SOURCES})
set_target_properties(decoder_sql PROPERTIES OUTPUT_NAME decoder_sql PREFIX "")
install(TARGETS decoder_sql DESTINATION ${PG_PKGLIBDIR})
