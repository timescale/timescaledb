add_library(${PROJECT_NAME}_test_plugin MODULE test_plugin.c)
set_target_properties(
  ${PROJECT_NAME}_test_plugin
  PROPERTIES POSITION_INDEPENDENT_CODE ON
             OUTPUT_NAME ${PROJECT_NAME}_test_plugin
             PREFIX "")
install(
  TARGETS ${PROJECT_NAME}_test_plugin
  DESTINATION ${PG_PKGLIBDIR}
  OPTIONAL)
