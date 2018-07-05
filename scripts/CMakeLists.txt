if (APPLE)
  # We strictly need gobjdump on macs, because objdump is usually
  # llvm's incompatible objdump
  find_program(OBJDUMP NAMES gobjdump PATHS /usr/local/bin /opt/local/bin)
else ()
  find_program(OBJDUMP NAMES gobjdump objdump)
endif (APPLE)

if (OBJDUMP)
  message(STATUS "Using objdump ${OBJDUMP}")
else ()
  message(STATUS "Install objdump or gobjdump to be able to run pgindent")
endif (OBJDUMP)
configure_file(generate_typedefs.sh.in generate_typedefs.sh @ONLY)
