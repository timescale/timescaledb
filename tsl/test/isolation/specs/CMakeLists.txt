set(TEST_TEMPLATES
 reorder_deadlock.spec.in
 reorder_vs_insert_other_chunk.spec.in
 reorder_vs_insert.spec.in
 reorder_vs_select.spec.in)

foreach(TEMPLATE_FILE ${TEST_TEMPLATES})
  get_filename_component(TEMPLATE ${TEMPLATE_FILE} NAME_WE)
  set(TEST_FILE ${TEMPLATE}.spec)
  configure_file(${TEMPLATE_FILE} ${CMAKE_CURRENT_SOURCE_DIR}/${TEST_FILE})
  list(APPEND TEST_FILES ${TEST_FILE})
  add_custom_command(
    OUTPUT ${TEST_FILE}
    DEPENDS ${TEMPLATE_FILE}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${CAT_CMD}
    COMMENT "Generating ${OUTPUT_FILE}"
  )
endforeach(TEMPLATE_FILE)

add_custom_target(tslisolationtests ALL DEPENDS ${TEST_TEMPLATES})
