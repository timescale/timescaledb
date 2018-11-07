if (APPLE)
  # we need to use dwarfdump on mac b/c the gobjdump is occasionally broken
  # and llvm-objdump doesn't output the information we need.
  find_program(OBJDUMP NAMES dwarfdump)
else ()
  find_program(OBJDUMP NAMES gobjdump objdump)
endif (APPLE)

if (OBJDUMP)
  message(STATUS "Using objdump ${OBJDUMP}")
else ()
  message(STATUS "Install objdump or gobjdump to be able to run pgindent")
endif (OBJDUMP)
configure_file(generate_typedefs.sh.in generate_typedefs.sh @ONLY)
