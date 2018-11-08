find_program(NM NAMES nm PATHS /usr/bin /usr/local/bin /opt/local/bin)
if(NM)
  message(STATUS "Using nm ${NM}")
else ()
  message(STATUS "Install nm to be able to run export checks")
endif (NM)

configure_file(export_prefix_check.sh.in export_prefix_check.sh @ONLY)
