include_directories(${CMAKE_CURRENT_SOURCE_DIR})

## generate log_code.cc, log_print.cc, log_header.cc
set_source_files_properties(
  "${CMAKE_CURRENT_BINARY_DIR}/log_code.cc"
  "${CMAKE_CURRENT_BINARY_DIR}/log_print.cc"
  "${CMAKE_CURRENT_BINARY_DIR}/log_header.h"
  PROPERTIES GENERATED TRUE)

add_executable(logformat logger/logformat.cc)
target_link_libraries(logformat ${LIBTOKUPORTABILITY}_static)
if (USE_GCOV)
   add_space_separated_property(TARGET logformat LINK_FLAGS --coverage)
endif (USE_GCOV)

add_custom_command(
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/log_code.cc"
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/log_print.cc"
  OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/log_header.h"
  COMMAND $<TARGET_FILE:logformat> .
  DEPENDS logger/logformat
  )
add_custom_target(
  generate_log_code
  DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/log_code.cc" "${CMAKE_CURRENT_BINARY_DIR}/log_print.cc" "${CMAKE_CURRENT_BINARY_DIR}/log_header.h"
  )

set(FT_SOURCES
  bndata.cc
  cachetable/background_job_manager.cc
  cachetable/cachetable.cc
  cachetable/checkpoint.cc
  cursor.cc
  ft.cc
  ft-cachetable-wrappers.cc
  ft-flusher.cc
  ft-hot-flusher.cc
  ft-ops.cc
  ft-recount-rows.cc
  ft-status.cc
  ft-test-helpers.cc
  ft-verify.cc
  loader/callbacks.cc
  loader/dbufio.cc
  loader/loader.cc
  loader/pqueue.cc
  leafentry.cc
  le-cursor.cc
  logger/logcursor.cc
  logger/logfilemgr.cc
  logger/logger.cc
  logger/log_upgrade.cc
  logger/recover.cc
  msg.cc
  msg_buffer.cc
  node.cc
  pivotkeys.cc
  serialize/rbtree_mhs.cc
  serialize/block_allocator.cc
  serialize/block_table.cc
  serialize/compress.cc
  serialize/ft_node-serialize.cc
  serialize/ft-node-deserialize.cc
  serialize/ft-serialize.cc
  serialize/quicklz.cc
  serialize/sub_block.cc
  txn/rollback.cc
  txn/rollback-apply.cc
  txn/rollback-ct-callbacks.cc
  txn/rollback_log_node_cache.cc
  txn/roll.cc
  txn/txn.cc
  txn/txn_child_manager.cc
  txn/txn_manager.cc
  txn/xids.cc
  ule.cc
  "${CMAKE_CURRENT_BINARY_DIR}/log_code.cc"
  "${CMAKE_CURRENT_BINARY_DIR}/log_print.cc"
  )

add_library(ft SHARED ${FT_SOURCES})
add_library(ft_static STATIC ${FT_SOURCES})
## we're going to link this into libtokudb.so so it needs to have PIC
set_target_properties(ft_static PROPERTIES POSITION_INDEPENDENT_CODE ON)
maybe_add_gcov_to_libraries(ft ft_static)

## depend on other generated targets
add_dependencies(ft install_tdb_h generate_log_code build_lzma build_snappy)
add_dependencies(ft_static install_tdb_h generate_log_code build_lzma build_snappy)

## link with lzma (which should be static) and link dependers with zlib
target_link_libraries(ft LINK_PRIVATE util_static lzma snappy ${LIBTOKUPORTABILITY})
target_link_libraries(ft LINK_PUBLIC z)
target_link_libraries(ft_static LINK_PRIVATE lzma snappy)

add_subdirectory(tests)
