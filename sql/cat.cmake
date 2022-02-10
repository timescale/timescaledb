
IF(POLICY CMP0012)
  CMAKE_POLICY(SET CMP0012 NEW)
ENDIF()

if (NOT DEFINED STRIP_REPLACE)
  set(STRIP_REPLACE OFF)
endif()

function(append_file IN_FILE OUT_FILE STRIP_REPLACE)
  file(READ ${IN_FILE} CONTENTS)
  if (${STRIP_REPLACE})
    string(REPLACE " OR REPLACE " " " CONTENTS "${CONTENTS}")
  endif()
  file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction()

# Function to concatenate all files in SRC_FILE_LIST into file OUTPUT_FILE
function(cat SRC_FILE_LIST OUTPUT_FILE STRIP_REPLACE)
  file(WRITE ${OUTPUT_FILE} "")
  foreach(SRC_FILE ${SRC_FILE_LIST})
    append_file(${SRC_FILE} ${OUTPUT_FILE} ${STRIP_REPLACE})
  endforeach()
endfunction()

cat("${SRC_FILE_LIST}" "${OUTPUT_FILE}" "${STRIP_REPLACE}")
