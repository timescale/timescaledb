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
endforeach(TEMPLATE_FILE)
