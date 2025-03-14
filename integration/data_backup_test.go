package integration

import (
	"fmt"
	"os"
	"path"

	"github.com/GreengageDB/gp-common-go-libs/gplog"
	"github.com/GreengageDB/gp-common-go-libs/testhelper"
	"github.com/blang/semver"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup integration tests", func() {
	Describe("BackupDataForAllTables", func() {
		var (
			testTables             []backup.Table
			fpInfo                 filepath.FilePathInfo
			origFPInfo             filepath.FilePathInfo
			origPipeThroughProgram utils.PipeThroughProgram
		)
		BeforeEach(func() {
			if connectionPool.Version.Before(backup.SNAPSHOT_GPDB_MIN_VERSION) {
				Skip(fmt.Sprintf("Test only applicable to GPDB %s and above", backup.SNAPSHOT_GPDB_MIN_VERSION))
			}
			if useOldBackupVersion && oldBackupSemVer.LT(semver.MustParse("1.29.0")) {
				Skip(fmt.Sprintf("Test does not apply to gpbackup gpbackup %s", oldBackupSemVer))
			}

			fpInfo = filepath.NewFilePathInfo(testCluster, "", "20170101010101", "gpseg", false)
			fpInfo.BaseDataDir = "/tmp/backup_data_test"
			os.MkdirAll(path.Join(fpInfo.BaseDataDir, "backups", "20170101", "20170101010101"), 0777)

			backup.SetCluster(testCluster)

			origFPInfo = backup.GetFPInfo()
			backup.SetFPInfo(fpInfo)

			origPipeThroughProgram = utils.GetPipeThroughProgram()

			testhelper.AssertQueryRuns(connectionPool, `CREATE SCHEMA dataTest;`)
			// Set to Verbose so progress bars won't show when running tests
			gplog.SetVerbosity(gplog.LOGVERBOSE)
		})
		AfterEach(func() {
			testhelper.AssertQueryRuns(connectionPool, `DROP SCHEMA dataTest CASCADE;`)
			os.RemoveAll(fpInfo.BaseDataDir)
			backup.SetFPInfo(origFPInfo)
			utils.SetPipeThroughProgram(origPipeThroughProgram)
			gplog.SetVerbosity(gplog.LOGINFO)
		})
		It("backs up multiple tables with valid data", FlakeAttempts(5), func() {

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.testtable1 (i int) DISTRIBUTED BY (i);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.testtable2 (i int) DISTRIBUTED BY (i);`)
			testTables = []backup.Table{{
				Relation:        backup.Relation{Oid: 0, Schema: "dataTest", Name: "testtable1"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			},
				{
					Relation:        backup.Relation{Oid: 1, Schema: "dataTest", Name: "testtable2"},
					TableDefinition: backup.TableDefinition{IsExternal: false},
				},
			}
			// set up a backupsnapshot to ensure the code flow we're testing is the intended case
			connectionPool.MustBegin()
			defer connectionPool.MustCommit()
			testSnapshot, err := backup.GetSynchronizedSnapshot(connectionPool)
			Expect(err).ToNot(HaveOccurred())
			backup.SetBackupSnapshot(testSnapshot)
			Expect(err).ToNot(HaveOccurred())
			// Open new transaction if it was not done before
			if connectionPool.Tx[1] == nil {
				connectionPool.MustBegin(1)
			}
			// Ensure transaction is closed
			connectionPool.MustCommit(1)
			// Open new transaction with shared snapshot
			err = backup.SetSynchronizedSnapshot(connectionPool, 1, testSnapshot)
			Expect(err).ToNot(HaveOccurred())
			// Insert values into both tables
			_, err = connectionPool.Exec(fmt.Sprintf(`INSERT INTO dataTest.testtable1 VALUES (1);`), 1)
			Expect(err).To(BeNil(), "%s", fmt.Sprintf(`INSERT INTO dataTest.testtable1 VALUES (1);`), 1)
			_, err = connectionPool.Exec(fmt.Sprintf(`INSERT INTO dataTest.testtable2 VALUES (1);`), 1)
			Expect(err).To(BeNil(), "%s", fmt.Sprintf(`INSERT INTO dataTest.testtable2 VALUES (1);`), 1)

			Expect(func() {
				rowsCopiedMaps := backup.BackupDataForAllTables(testTables)
				// Ensure gpbackup does not see inserted after snapshot values
				Expect(rowsCopiedMaps[1][0]).To(Equal(int64(0)))
				Expect(rowsCopiedMaps[1][1]).To(Equal(int64(0)))
			}).ShouldNot(Panic())

			// Assert that at least one segment's worth of files for both tables were written out
			_, err = os.Stat("/tmp/backup_data_test/backups/20170101/20170101010101/gpbackup_0_20170101010101_0")
			Expect(err).ToNot(HaveOccurred())
			_, err = os.Stat("/tmp/backup_data_test/backups/20170101/20170101010101/gpbackup_0_20170101010101_1")
			Expect(err).ToNot(HaveOccurred())
		})
		It("correctly errors if a piped copy command fails", func() {
			// We had a bug for a while where this would result in a permanent hang, instead of an
			// error. This coverage is meant to prevent that from reocurring in future refactors,
			// which that function needs.
			dummyPipeThrough := utils.PipeThroughProgram{
				Name:          "dummy",
				OutputCommand: "doesnotexist",
				InputCommand:  "doesnotexist",
				Extension:     ".dne",
			}
			utils.SetPipeThroughProgram(dummyPipeThrough)

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.test2table1 (i int) DISTRIBUTED BY (i);`)
			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.test2table2 (i int) DISTRIBUTED BY (i);`)
			testTables = []backup.Table{{
				Relation:        backup.Relation{Oid: 0, Schema: "dataTest", Name: "test2table1"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			},
				{
					Relation:        backup.Relation{Oid: 1, Schema: "dataTest", Name: "test2table2"},
					TableDefinition: backup.TableDefinition{IsExternal: false},
				},
			}
			connectionPool.MustBegin()
			defer connectionPool.MustRollback()

			testSnapshot, err := backup.GetSynchronizedSnapshot(connectionPool)
			Expect(err).ToNot(HaveOccurred())
			backup.SetBackupSnapshot(testSnapshot)

			Expect(func() { backup.BackupDataForAllTables(testTables) }).Should(Panic())

			// Terminate the hanging copy command or it breaks test suite cleanup. We do not need
			// to worry about GPDB5- syntax here because these tests don't apply to that
			cleanupConn := testutils.SetupTestDbConn("testdb")
			defer cleanupConn.Close()
			query := `
			    SELECT pg_terminate_backend(pid)
			    FROM pg_stat_activity
			    WHERE application_name = ''
			    AND query like '%COPY%PROGRAM%doesnotexist%'
                AND pid <> pg_backend_pid();`
			cleanupConn.MustExec(query)
		})

		It("correctly errors if an unexpected error occurs taking a lock", func() {

			testhelper.AssertQueryRuns(connectionPool, `CREATE TABLE dataTest.test3table1 (i int) DISTRIBUTED BY (i);`)
			// do not create second table, so taking a lock on it will error
			testTables = []backup.Table{{
				Relation:        backup.Relation{Oid: 0, Schema: "dataTest", Name: "test3table1"},
				TableDefinition: backup.TableDefinition{IsExternal: false},
			},
				{
					Relation:        backup.Relation{Oid: 1, Schema: "dataTest", Name: "test3table2"},
					TableDefinition: backup.TableDefinition{IsExternal: false},
				},
			}
			connectionPool.MustBegin()
			defer connectionPool.MustRollback()

			testSnapshot, err := backup.GetSynchronizedSnapshot(connectionPool)
			Expect(err).ToNot(HaveOccurred())
			backup.SetBackupSnapshot(testSnapshot)

			Expect(func() { backup.BackupDataForAllTables(testTables) }).Should(Panic())
		})
	})
})
