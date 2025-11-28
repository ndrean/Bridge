const std = @import("std");

/// Link vendored libpq static libraries to an executable
fn linkLibpq(exe: *std.Build.Step.Compile, b: *std.Build) void {
    // Add include path for headers
    exe.addIncludePath(b.path("libs/libpq-install/include"));

    // Add library path
    exe.addLibraryPath(b.path("libs/libpq-install/lib"));

    // Link the static libraries in correct order: pgcommon and pgport first, then pq
    // This is important because libpq depends on symbols from pgcommon and pgport
    exe.addObjectFile(b.path("libs/libpq-install/lib/libpgcommon.a"));
    exe.addObjectFile(b.path("libs/libpq-install/lib/libpgport.a"));
    exe.addObjectFile(b.path("libs/libpq-install/lib/libpq.a"));

    // Link system dependencies that libpq needs
    exe.linkLibC();
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const mod = b.addModule("bridge", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
    });

    const pg = b.dependency("pg", .{
        .target = target,
        .optimize = optimize,
    });

    const msgpack = b.dependency("zig_msgpack", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "bridge",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),

            .target = target,
            .optimize = optimize,

            .imports = &.{
                .{ .name = "bridge", .module = mod },
                .{ .name = "pg", .module = pg.module("pg") },
                .{ .name = "msgpack", .module = msgpack.module("msgpack") },
            },
        }),
    });

    // Link against pre-built NATS C library
    // Build the library first by running: ./build_nats.sh
    exe.addIncludePath(b.path("libs/nats-install/include"));
    exe.addLibraryPath(b.path("libs/nats-install/lib"));
    exe.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    exe.linkLibC();

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");

    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);

    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const mod_tests = b.addTest(.{
        .root_module = mod,
    });

    // A run step that will run the test executable.
    const run_mod_tests = b.addRunArtifact(mod_tests);

    // Creates an executable that will run `test` blocks from the executable's
    // root module. Note that test executables only test one module at a time,
    // hence why we have to create two separate ones.
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });

    // A run step that will run the second test executable.
    const run_exe_tests = b.addRunArtifact(exe_tests);

    // A top level step for running all tests. dependOn can be called multiple
    // times and since the two run steps do not depend on one another, this will
    // make the two of them run in parallel.
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    // ===== Commented out test executables - files moved to src/test_files/ =====
    // Uncomment and update paths if needed for testing
    //
    // // NATS test executable
    // const nats_test = b.addExecutable(.{
    //     .name = "nats_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/nats_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //     }),
    // });
    // nats_test.addIncludePath(b.path("libs/nats-install/include"));
    // nats_test.addLibraryPath(b.path("libs/nats-install/lib"));
    // nats_test.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // nats_test.linkLibC();
    // b.installArtifact(nats_test);
    //
    // const nats_test_step = b.step("nats-test", "Test NATS connection");
    // const nats_test_run = b.addRunArtifact(nats_test);
    // nats_test_run.step.dependOn(b.getInstallStep());
    // nats_test_step.dependOn(&nats_test_run.step);
    //
    // // PostgreSQL test executable
    // const pg_test = b.addExecutable(.{
    //     .name = "pg_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/pg_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // b.installArtifact(pg_test);
    //
    // const pg_test_step = b.step("pg-test", "Test PostgreSQL connection");
    // const pg_test_run = b.addRunArtifact(pg_test);
    // pg_test_run.step.dependOn(b.getInstallStep());
    // pg_test_step.dependOn(&pg_test_run.step);
    //
    // // CDC Demo executable
    // const cdc_demo = b.addExecutable(.{
    //     .name = "cdc_demo",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/cdc_demo.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //             .{ .name = "msgpack", .module = msgpack.module("msgpack") },
    //         },
    //     }),
    // });
    // cdc_demo.addIncludePath(b.path("libs/nats-install/include"));
    // cdc_demo.addLibraryPath(b.path("libs/nats-install/lib"));
    // cdc_demo.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // cdc_demo.linkLibC();
    // b.installArtifact(cdc_demo);
    //
    // const cdc_demo_step = b.step("cdc-demo", "Run CDC bridge demo");
    // const cdc_demo_run = b.addRunArtifact(cdc_demo);
    // cdc_demo_run.step.dependOn(b.getInstallStep());
    // cdc_demo_step.dependOn(&cdc_demo_run.step);
    //
    // // CDC Load Test executable
    // const cdc_load_test = b.addExecutable(.{
    //     .name = "cdc_load_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/cdc_load_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //             .{ .name = "msgpack", .module = msgpack.module("msgpack") },
    //         },
    //     }),
    // });
    // // Link NATS
    // cdc_load_test.addIncludePath(b.path("libs/nats-install/include"));
    // cdc_load_test.addLibraryPath(b.path("libs/nats-install/lib"));
    // cdc_load_test.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // // Link vendored libpq
    // linkLibpq(cdc_load_test, b);
    // b.installArtifact(cdc_load_test);
    //
    // const cdc_load_test_step = b.step("load-test", "Run CDC load test");
    // const cdc_load_test_run = b.addRunArtifact(cdc_load_test);
    // cdc_load_test_run.step.dependOn(b.getInstallStep());
    // cdc_load_test_step.dependOn(&cdc_load_test_run.step);
    //
    // // WAL Stream Test executable
    // const wal_stream_test = b.addExecutable(.{
    //     .name = "wal_stream_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/wal_stream_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // // Link vendored libpq for replication protocol
    // linkLibpq(wal_stream_test, b);
    // b.installArtifact(wal_stream_test);
    //
    // const wal_stream_test_step = b.step("wal-stream-test", "Test WAL streaming with libpq");
    // const wal_stream_test_run = b.addRunArtifact(wal_stream_test);
    // wal_stream_test_run.step.dependOn(b.getInstallStep());
    // wal_stream_test_step.dependOn(&wal_stream_test_run.step);

    // Bridge Demo - End-to-end CDC to NATS
    const bridge_demo = b.addExecutable(.{
        .name = "bridge_demo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/bridge_demo.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "pg", .module = pg.module("pg") },
                .{ .name = "msgpack", .module = msgpack.module("msgpack") },
            },
        }),
    });
    // Link NATS
    bridge_demo.addIncludePath(b.path("libs/nats-install/include"));
    bridge_demo.addLibraryPath(b.path("libs/nats-install/lib"));
    bridge_demo.addObjectFile(b.path("libs/nats-install/lib/libnats_static.a"));
    // Link vendored libpq
    linkLibpq(bridge_demo, b);
    b.installArtifact(bridge_demo);

    const bridge_demo_step = b.step("bridge-demo", "Run end-to-end CDC bridge demo");
    const bridge_demo_run = b.addRunArtifact(bridge_demo);
    bridge_demo_run.step.dependOn(b.getInstallStep());
    bridge_demo_step.dependOn(&bridge_demo_run.step);

    // // Connection State Test
    // const conn_state_test = b.addExecutable(.{
    //     .name = "connection_state_test",
    //     .root_module = b.createModule(.{
    //         .root_source_file = b.path("src/connection_state_test.zig"),
    //         .target = target,
    //         .optimize = optimize,
    //         .imports = &.{
    //             .{ .name = "pg", .module = pg.module("pg") },
    //         },
    //     }),
    // });
    // b.installArtifact(conn_state_test);
    //
    // const conn_state_test_step = b.step("conn-test", "Test connection state handling");
    // const conn_state_test_run = b.addRunArtifact(conn_state_test);
    // conn_state_test_run.step.dependOn(b.getInstallStep());
    // conn_state_test_step.dependOn(&conn_state_test_run.step);
}
