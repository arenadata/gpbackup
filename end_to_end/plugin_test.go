package end_to_end_test

import (
	"fmt"
	"os"
	"os/exec"
	path "path/filepath"
	"time"

	"github.com/GreengageDB/gp-common-go-libs/cluster"
	"github.com/GreengageDB/gp-common-go-libs/dbconn"
	"github.com/GreengageDB/gp-common-go-libs/iohelper"
	"github.com/GreengageDB/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func copyPluginToAllHosts(conn *dbconn.DBConn, pluginPath string) {
	hostnameQuery := `SELECT DISTINCT hostname AS string FROM gp_segment_configuration WHERE content != -1`
	hostnames := dbconn.MustSelectStringSlice(conn, hostnameQuery)
	for _, hostname := range hostnames {
		// Skip the local host
		h, _ := os.Hostname()
		if hostname == h {
			continue
		}
		examplePluginTestDir, _ := path.Split(pluginPath)
		command := exec.Command("ssh", hostname, fmt.Sprintf("mkdir -p %s", examplePluginTestDir))
		mustRunCommand(command)
		command = exec.Command("scp", pluginPath, fmt.Sprintf("%s:%s", hostname, pluginPath))
		mustRunCommand(command)
	}
}

func forceMetadataFileDownloadFromPlugin(conn *dbconn.DBConn, timestamp string) {
	fpInfo := filepath.NewFilePathInfo(backupCluster, "", timestamp, "", false)
	remoteOutput := backupCluster.GenerateAndExecuteCommand(
		fmt.Sprintf("Removing backups on all segments for "+
			"timestamp %s", timestamp),
		cluster.ON_SEGMENTS|cluster.INCLUDE_COORDINATOR,
		func(contentID int) string {
			return fmt.Sprintf("rm -rf %s", fpInfo.GetDirForContent(contentID))
		})
	if remoteOutput.NumErrors != 0 {
		Fail(fmt.Sprintf("Failed to remove backup directory for timestamp %s", timestamp))
	}
}

var _ = Describe("End to End plugin tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	Describe("Single data file", func() {
		It("runs gpbackup and gprestore with single-data-file flag", func() {
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--single-data-file",
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir)

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertArtifactsCleaned(timestamp)

		})
		It("runs gpbackup and gprestore with single-data-file flag with copy-queue-size", func() {
			skipIfOldBackupVersionBefore("1.23.0")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--single-data-file",
				"--copy-queue-size", "4",
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--copy-queue-size", "4",
				"--backup-dir", backupDir)

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertArtifactsCleaned(timestamp)

		})
		It("runs gpbackup and gprestore with single-data-file flag without compression", func() {
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--single-data-file",
				"--backup-dir", backupDir,
				"--no-compression")
			timestamp := getBackupTimestamp(string(output))
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir)

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertArtifactsCleaned(timestamp)
		})
		It("runs gpbackup and gprestore with single-data-file flag without compression with copy-queue-size", func() {
			skipIfOldBackupVersionBefore("1.23.0")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--single-data-file",
				"--copy-queue-size", "4",
				"--backup-dir", backupDir,
				"--no-compression")
			timestamp := getBackupTimestamp(string(output))
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--copy-queue-size", "4",
				"--backup-dir", backupDir)

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
			assertArtifactsCleaned(timestamp)
		})
		It("runs gpbackup and gprestore on database with all objects", func() {
			schemaResetStatements := "DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
			testhelper.AssertQueryRuns(backupConn, schemaResetStatements)
			defer testutils.ExecuteSQLFile(backupConn,
				"resources/test_tables_data.sql")
			defer testutils.ExecuteSQLFile(backupConn,
				"resources/test_tables_ddl.sql")
			defer testhelper.AssertQueryRuns(backupConn, schemaResetStatements)
			defer testhelper.AssertQueryRuns(restoreConn, schemaResetStatements)
			testhelper.AssertQueryRuns(backupConn,
				"CREATE ROLE testrole SUPERUSER")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP ROLE testrole")

			// In GPDB 7+, we use plpython3u because of default python 3 support.
			plpythonDropStatement := "DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;"
			if backupConn.Version.AtLeast("7") {
				plpythonDropStatement = "DROP PROCEDURAL LANGUAGE IF EXISTS plpython3u;"
			}
			testhelper.AssertQueryRuns(backupConn, plpythonDropStatement)
			defer testhelper.AssertQueryRuns(backupConn, plpythonDropStatement)
			defer testhelper.AssertQueryRuns(restoreConn, plpythonDropStatement)

			testutils.ExecuteSQLFile(backupConn, "resources/gpdb4_objects.sql")
			if backupConn.Version.Before("7") {
				testutils.ExecuteSQLFile(backupConn, "resources/gpdb4_compatible_objects_before_gpdb7.sql")
			} else {
				testutils.ExecuteSQLFile(backupConn, "resources/gpdb4_compatible_objects_after_gpdb7.sql")
			}

			if backupConn.Version.AtLeast("5") {
				testutils.ExecuteSQLFile(backupConn, "resources/gpdb5_objects.sql")
			}
			if backupConn.Version.AtLeast("6") {
				testutils.ExecuteSQLFile(backupConn, "resources/gpdb6_objects.sql")
				defer testhelper.AssertQueryRuns(backupConn,
					"DROP FOREIGN DATA WRAPPER fdw CASCADE;")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP FOREIGN DATA WRAPPER fdw CASCADE;")
			}
			if backupConn.Version.AtLeast("6.2") {
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE mview_table1(i int, j text);")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP TABLE mview_table1;")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE MATERIALIZED VIEW mview1 (i2) as select i from mview_table1;")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP MATERIALIZED VIEW mview1;")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE MATERIALIZED VIEW mview2 as select * from mview1;")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP MATERIALIZED VIEW mview2;")
			}

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data",
				"--single-data-file")
			timestamp := getBackupTimestamp(string(output))
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--metadata-only",
				"--redirect-db", "restoredb")
			assertArtifactsCleaned(timestamp)
		})
		It("runs gpbackup and gprestore on database with all objects with copy-queue-size", func() {
			skipIfOldBackupVersionBefore("1.23.0")
			testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			defer testutils.ExecuteSQLFile(backupConn,
				"resources/test_tables_data.sql")
			defer testutils.ExecuteSQLFile(backupConn,
				"resources/test_tables_ddl.sql")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			defer testhelper.AssertQueryRuns(restoreConn,
				"DROP SCHEMA IF EXISTS schema2 CASCADE; DROP SCHEMA public CASCADE; CREATE SCHEMA public; DROP PROCEDURAL LANGUAGE IF EXISTS plpythonu;")
			testhelper.AssertQueryRuns(backupConn,
				"CREATE ROLE testrole SUPERUSER")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP ROLE testrole")
			testutils.ExecuteSQLFile(backupConn, "resources/gpdb4_objects.sql")
			if backupConn.Version.AtLeast("5") {
				testutils.ExecuteSQLFile(backupConn, "resources/gpdb5_objects.sql")
			}
			if backupConn.Version.AtLeast("6") {
				testutils.ExecuteSQLFile(backupConn, "resources/gpdb6_objects.sql")
				defer testhelper.AssertQueryRuns(backupConn,
					"DROP FOREIGN DATA WRAPPER fdw CASCADE;")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP FOREIGN DATA WRAPPER fdw CASCADE;")
			}
			if backupConn.Version.AtLeast("6.2") {
				testhelper.AssertQueryRuns(backupConn,
					"CREATE TABLE mview_table1(i int, j text);")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP TABLE mview_table1;")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE MATERIALIZED VIEW mview1 (i2) as select i from mview_table1;")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP MATERIALIZED VIEW mview1;")
				testhelper.AssertQueryRuns(backupConn,
					"CREATE MATERIALIZED VIEW mview2 as select * from mview1;")
				defer testhelper.AssertQueryRuns(restoreConn,
					"DROP MATERIALIZED VIEW mview2;")
			}

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data",
				"--single-data-file",
				"--copy-queue-size", "4")
			timestamp := getBackupTimestamp(string(output))
			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--metadata-only",
				"--redirect-db", "restoredb",
				"--copy-queue-size", "4")
			assertArtifactsCleaned(timestamp)
		})

		Context("with include filtering on restore", func() {
			It("runs gpbackup and gprestore with include-table-file restore flag with a single data file", func() {
				includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
				utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--backup-dir", backupDir,
					"--single-data-file")
				timestamp := getBackupTimestamp(string(output))
				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--backup-dir", backupDir,
					"--include-table-file", "/tmp/include-tables.txt")
				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{
					"public.sales": 13, "public.foo": 40000})
				assertArtifactsCleaned(timestamp)

				_ = os.Remove("/tmp/include-tables.txt")
			})
			It("runs gpbackup and gprestore with include-table-file restore flag with a single data with copy-queue-size", func() {
				skipIfOldBackupVersionBefore("1.23.0")
				includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
				utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--backup-dir", backupDir,
					"--single-data-file",
					"--copy-queue-size", "4")
				timestamp := getBackupTimestamp(string(output))
				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--backup-dir", backupDir,
					"--include-table-file", "/tmp/include-tables.txt",
					"--copy-queue-size", "4")
				assertRelationsCreated(restoreConn, 16)
				assertDataRestored(restoreConn, map[string]int{
					"public.sales": 13, "public.foo": 40000})
				assertArtifactsCleaned(timestamp)

				_ = os.Remove("/tmp/include-tables.txt")
			})
			It("runs gpbackup and gprestore with include-schema restore flag with a single data file", func() {
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--backup-dir", backupDir,
					"--single-data-file")
				timestamp := getBackupTimestamp(string(output))
				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--backup-dir", backupDir,
					"--include-schema", "schema2")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(timestamp)
			})
			It("runs gpbackup and gprestore with include-schema restore flag with a single data file with copy-queue-size", func() {
				skipIfOldBackupVersionBefore("1.23.0")
				output := gpbackup(gpbackupPath, backupHelperPath,
					"--backup-dir", backupDir,
					"--single-data-file",
					"--copy-queue-size", "4")
				timestamp := getBackupTimestamp(string(output))
				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--backup-dir", backupDir,
					"--include-schema", "schema2",
					"--copy-queue-size", "4")

				assertRelationsCreated(restoreConn, 17)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(timestamp)
			})
		})

		Context("with plugin", func() {
			BeforeEach(func() {
				skipIfOldBackupVersionBefore("1.7.0")
				// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
				if useOldBackupVersion {
					Skip("This test is only needed for the most recent backup versions")
				}
			})
			It("Will not hang if gpbackup and gprestore runs with single-data-file and the helper goes down at its start", func(ctx SpecContext) {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				testhelper.AssertQueryRuns(backupConn, "CREATE TABLE t0(a int);")
				testhelper.AssertQueryRuns(backupConn, "INSERT INTO t0 SELECT i FROM generate_series(1, 10)i;")
				defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE t0;")

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--single-data-file",
					"--plugin-config", examplePluginTestConfig)
				timestamp := getBackupTimestamp(string(output))

				backupCluster.GenerateAndExecuteCommand(
					"Instruct plugin to fail",
					cluster.ON_HOSTS,
					func(contentID int) string {
						return fmt.Sprintf("touch /tmp/GPBACKUP_PLUGIN_DIE")
					})

				defer backupCluster.GenerateAndExecuteCommand(
					"Unset plugin instruction",
					cluster.ON_HOSTS,
					func(contentID int) string {
						return fmt.Sprintf("rm /tmp/GPBACKUP_PLUGIN_DIE")
					})

				gprestoreCmd := exec.Command(gprestorePath,
					"--timestamp", timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig)

				_, err := gprestoreCmd.CombinedOutput()
				Expect(err).To(HaveOccurred())

				assertArtifactsCleaned(timestamp)
			}, SpecTimeout(time.Second*30))
			It("Will not hang if gprestore runs with cluster resize and the helper goes down on one of the tables", func(ctx SpecContext) {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				pluginBackupDirectory := `/tmp/plugin_dest`
				os.Mkdir(pluginBackupDirectory, 0777)
				command := exec.Command("tar", "-xzf", fmt.Sprintf("resources/%s.tar.gz", "9-segment-db-with-plugin"), "-C", pluginBackupDirectory)
				mustRunCommand(command)

				backupCluster.GenerateAndExecuteCommand(
					"Instruct plugin to fail",
					cluster.ON_HOSTS,
					func(contentID int) string {
						return fmt.Sprintf("touch /tmp/GPBACKUP_PLUGIN_DIE")
					})

				defer backupCluster.GenerateAndExecuteCommand(
					"Unset plugin instruction",
					cluster.ON_HOSTS,
					func(contentID int) string {
						return fmt.Sprintf("rm /tmp/GPBACKUP_PLUGIN_DIE")
					})

				timestamp := "20240812201233"

				gprestoreCmd := exec.Command(gprestorePath,
					"--resize-cluster",
					"--timestamp", timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig)

				// instruct plugin to die only before restoring the last table
				gprestoreCmd.Env = os.Environ()
				gprestoreCmd.Env = append(gprestoreCmd.Env, "GPBACKUP_PLUGIN_DIE_ON_OID=16392")

				_, err := gprestoreCmd.CombinedOutput()
				Expect(err).To(HaveOccurred())

				assertArtifactsCleaned(timestamp)

				os.RemoveAll(pluginBackupDirectory)
			}, SpecTimeout(time.Second*30))
			It("Will not hang on error with plugin during resize-restore and on-error-continue", func(ctx SpecContext) {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				pluginBackupDirectory := `/tmp/plugin_dest`
				os.Mkdir(pluginBackupDirectory, 0777)
				command := exec.Command("tar", "-xzf", fmt.Sprintf("resources/%s.tar.gz", "9-segment-db-with-plugin-error"), "-C", pluginBackupDirectory)
				mustRunCommand(command)

				timestamp := "20241121122453"

				gprestoreCmd := exec.Command(gprestorePath,
					"--resize-cluster",
					"--timestamp", timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig,
					"--on-error-continue")

				_, err := gprestoreCmd.CombinedOutput()
				Expect(err).To(HaveOccurred())

				assertArtifactsCleaned(timestamp)

				os.RemoveAll(pluginBackupDirectory)
			}, SpecTimeout(time.Second*30))
			It("runs gpbackup and gprestore with plugin, single-data-file, and no-compression", func() {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--single-data-file",
					"--no-compression",
					"--plugin-config", examplePluginTestConfig)
				timestamp := getBackupTimestamp(string(output))
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(timestamp)
			})
			It("runs gpbackup and gprestore with plugin, single-data-file, no-compression, and copy-queue-size", func() {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--single-data-file",
					"--copy-queue-size", "4",
					"--no-compression",
					"--plugin-config", examplePluginTestConfig)
				timestamp := getBackupTimestamp(string(output))
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig,
					"--copy-queue-size", "4")

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(timestamp)
			})
			It("runs gpbackup and gprestore with plugin and single-data-file", func() {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--single-data-file",
					"--plugin-config", examplePluginTestConfig)
				timestamp := getBackupTimestamp(string(output))
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(timestamp)
			})
			It("runs gpbackup and gprestore with plugin, single-data-file, and copy-queue-size", func() {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--single-data-file",
					"--copy-queue-size", "4",
					"--plugin-config", examplePluginTestConfig)
				timestamp := getBackupTimestamp(string(output))
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig,
					"--copy-queue-size", "4")

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertDataRestored(restoreConn, publicSchemaTupleCounts)
				assertDataRestored(restoreConn, schema2TupleCounts)
				assertArtifactsCleaned(timestamp)
			})
			It("runs gpbackup and gprestore with plugin and metadata-only", func() {
				copyPluginToAllHosts(backupConn, examplePluginExec)

				output := gpbackup(gpbackupPath, backupHelperPath,
					"--metadata-only",
					"--plugin-config", examplePluginTestConfig)
				timestamp := getBackupTimestamp(string(output))
				forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

				gprestore(gprestorePath, restoreHelperPath, timestamp,
					"--redirect-db", "restoredb",
					"--plugin-config", examplePluginTestConfig)

				assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
				assertArtifactsCleaned(timestamp)
			})
		})
	})

	Describe("Multi-file Plugin", func() {
		It("runs gpbackup and gprestore with plugin and no-compression", func() {
			skipIfOldBackupVersionBefore("1.7.0")
			// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
			if useOldBackupVersion {
				Skip("This test is only needed for the most recent backup versions")
			}
			copyPluginToAllHosts(backupConn, examplePluginExec)

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--no-compression",
				"--plugin-config", examplePluginTestConfig)
			timestamp := getBackupTimestamp(string(output))
			forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--plugin-config", examplePluginTestConfig)

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with plugin and compression", func() {
			skipIfOldBackupVersionBefore("1.7.0")
			// FIXME: we are temporarily disabling these tests because we will be altering our backwards compatibility logic.
			if useOldBackupVersion {
				Skip("This test is only needed for the most recent backup versions")
			}
			copyPluginToAllHosts(backupConn, examplePluginExec)

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--plugin-config", examplePluginTestConfig)
			timestamp := getBackupTimestamp(string(output))
			forceMetadataFileDownloadFromPlugin(backupConn, timestamp)

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--plugin-config", examplePluginTestConfig)

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
	})

	Describe("Example Plugin", func() {
		It("runs example_plugin.bash with plugin_test", func() {
			if useOldBackupVersion {
				Skip("This test is only needed for the latest backup version")
			}
			copyPluginToAllHosts(backupConn, examplePluginExec)
			command := exec.Command("bash", "-c", fmt.Sprintf("%s/plugin_test.sh %s %s", examplePluginDir, examplePluginExec, examplePluginTestConfig))
			mustRunCommand(command)
		})
	})
})
