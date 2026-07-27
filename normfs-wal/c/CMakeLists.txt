cmake_minimum_required(VERSION 3.10)

project(normfs_wal_c VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -pedantic")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(NORMFS_UINTN_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../../uintn-rs/c/include"
    CACHE PATH "Path to the uintn C include directory")

add_library(normfs_wal_c STATIC
    src/wal_header.c
    src/crc32c.c
    src/wal_entry.c
)
target_include_directories(normfs_wal_c PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${NORMFS_UINTN_INCLUDE}>
    $<INSTALL_INTERFACE:include>
)

# The header and entry codecs are established by Frama-C WP rather than by
# executable tests. The CRC32C hardware fast paths use compiler intrinsics that
# WP cannot interpret, so the one remaining test checks them against the
# proven portable path.
enable_testing()
add_executable(test_crc32c tests/test_crc32c.c)
target_link_libraries(test_crc32c PRIVATE normfs_wal_c)
add_test(NAME crc32c COMMAND test_crc32c)

find_program(FRAMAC frama-c)
if(FRAMAC)
    include(ProcessorCount)
    ProcessorCount(NPROC)
    if(NPROC EQUAL 0)
        set(NPROC 4)
    endif()
    # The entry codec's quantified byte clauses are heavy; capped parallelism
    # keeps Z3 from timing out under load, so the proof stays deterministic.
    if(NPROC GREATER 2)
        set(NPROC 2)
    endif()

    # Pinned, not configurable: the deployment targets are amd64 and arm64,
    # both LP64, so one model covers them. Naming it still beats the default,
    # which is not the same across Frama-C versions.

    set(NORMFS_WAL_HEADER_WP_FCTS
        normfs_uintn_le64_read
        normfs_uintn_le64_write
        normfs_wal_header_field_size_is_valid
        normfs_wal_header_v1_validate
        normfs_wal_header_v1_size
        normfs_wal_header_v1_encode
        normfs_wal_header_v1_decode
        normfs_wal_header_peek_version
        normfs_wal_header_v1_roundtrip_holds)

    set(NORMFS_WAL_ENTRY_WP_FCTS
        normfs_wal_entry_read_le32
        normfs_wal_entry_write_le32
        normfs_wal_entry_v1_size
        normfs_wal_entry_v1_encode
        normfs_wal_entry_v1_decode
        normfs_wal_entry_v1_id
        normfs_wal_entry_v1_iter_next)

    # An empty fcts list drops -wp-fct entirely: WP then also schedules global
    # lemma goals, which a function list silently excludes.
    function(normfs_add_wp_target target source fcts comment)
        if(fcts STREQUAL "")
            set(fct_args)
        else()
            string(REPLACE ";" "," fcts_arg "${fcts}")
            set(fct_args -wp-fct ${fcts_arg})
        endif()
        add_custom_target(${target}
            COMMAND ${FRAMAC}
                -machdep gcc_x86_64
                -wp-report-json ${CMAKE_CURRENT_BINARY_DIR}/${target}.json
                -wp -wp-rte -wp-timeout 240
                -wp-prover alt-ergo,z3
                -wp-par ${NPROC}
                -wp-cache none
                -wp-smoke-tests
                -wp-msg-key shell
                -kernel-warn-key annot:missing-spec=inactive
                -wp-warn-key pedantic-assigns=inactive
                -wp-no-warn-memory-model
                -cpp-extra-args=-I${CMAKE_CURRENT_SOURCE_DIR}/include\ -I${NORMFS_UINTN_INCLUDE}\ ${ARGN}
                ${fct_args}
                ${source}
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            COMMENT "${comment}"
            VERBATIM
        )
    endfunction()

    normfs_add_wp_target(verify-wal-header src/wal_header.c
        "${NORMFS_WAL_HEADER_WP_FCTS}" "WP: normfs V1 WAL header")
    # NORMFS_CRC32C_PORTABLE_ONLY strips the intrinsic fast paths WP cannot
    # interpret; the differential test covers them instead. That leaves only
    # the two proved functions in the unit, so no allowlist is needed, and
    # omitting it schedules the fold_extend lemma goal too.
    normfs_add_wp_target(verify-crc32c src/crc32c.c
        "" "WP: normfs CRC32C (portable path)"
        -DNORMFS_CRC32C_PORTABLE_ONLY)
    normfs_add_wp_target(verify-wal-entry src/wal_entry.c
        "${NORMFS_WAL_ENTRY_WP_FCTS}" "WP: normfs V1 WAL entry")

    add_custom_target(verify)
    add_dependencies(verify verify-wal-header verify-crc32c verify-wal-entry)
endif()
