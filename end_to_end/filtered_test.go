package end_to_end_test

import (
	"os"

	"github.com/GreengageDB/gp-common-go-libs/iohelper"
	"github.com/GreengageDB/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("End to End Filtered tests", func() {
	BeforeEach(func() {
		end_to_end_setup()
	})
	AfterEach(func() {
		end_to_end_teardown()
	})

	Describe("Backup include filtering", func() {
		It("runs gpbackup and gprestore with include-schema backup flag and compression level", func() {
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--include-schema", "public",
				"--compression-level", "2")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, 20)
			assertDataRestored(restoreConn, publicSchemaTupleCounts)
			assertArtifactsCleaned(timestamp)
		})
		It("runs gpbackup and gprestore with include-table backup flag", func() {
			skipIfOldBackupVersionBefore("1.4.0")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--include-table", "public.foo",
				"--include-table", "public.sales",
				"--include-table", "public.myseq1",
				"--include-table", "public.myview1")

			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, 16)
			assertDataRestored(restoreConn, map[string]int{"public.foo": 40000})
		})
		It("runs gpbackup and gprestore with include-table-file backup flag", func() {
			skipIfOldBackupVersionBefore("1.4.0")
			includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile, "public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--include-table-file", "/tmp/include-tables.txt")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, 16)
			assertDataRestored(restoreConn, map[string]int{"public.sales": 13, "public.foo": 40000})

			_ = os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with include-schema-file backup flag", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			includeFile := iohelper.MustOpenFileForWriting("/tmp/include-schema.txt")
			utils.MustPrintln(includeFile, "public")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--include-schema-file", "/tmp/include-schema.txt")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, 20)

			_ = os.Remove("/tmp/include-schema.txt")
		})
		It("runs gpbackup with --include-table flag with partitions (non-special chars)", func() {
			testhelper.AssertQueryRuns(backupConn,
				`CREATE TABLE public.testparent (id int, rank int, year int, gender
char(1), count int )
DISTRIBUTED BY (id)
PARTITION BY LIST (gender)
( PARTITION girls VALUES ('F'),
  PARTITION boys VALUES ('M'),
  DEFAULT PARTITION other );
			`)
			defer testhelper.AssertQueryRuns(backupConn,
				`DROP TABLE public.testparent`)

			testhelper.AssertQueryRuns(backupConn,
				`insert into public.testparent values (1,1,1,'M',1)`)
			testhelper.AssertQueryRuns(backupConn,
				`insert into public.testparent values (0,0,0,'F',1)`)

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir,
				"--include-table", `public.testparent_1_prt_girls`,
				"--leaf-partition-data")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir)

			// When running against GPDB 7+, only the root partition and the included leaf partition
			// will be created due to the new flexible GPDB 7+ partitioning logic. For versions
			// before GPDB 7, there is only one big DDL for the entire partition table.
			if backupConn.Version.AtLeast("7") {
				assertRelationsCreated(restoreConn, 2)
			} else {
				assertRelationsCreated(restoreConn, 4)
			}

			localSchemaTupleCounts := map[string]int{
				`public.testparent_1_prt_girls`: 1,
				`public.testparent`:             1,
			}
			assertDataRestored(restoreConn, localSchemaTupleCounts)
			assertArtifactsCleaned(timestamp)
		})
		It("backs up a child table inheriting from a parent when only the parent is included", func() {
			skipIfOldBackupVersionBefore("1.29.0") // Inheritance behavior changed in this version
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.parent(a int);`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.parent`)
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.child() INHERITS (public.parent);`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.child`)
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.unrelated(c int);`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.unrelated`)

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir,
				"--include-table", `public.parent`,
				"--metadata-only")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir)

			assertTablesRestored(restoreConn, []string{"public.parent", "public.child"})
			assertTablesNotRestored(restoreConn, []string{"public.unrelated"})
			assertArtifactsCleaned(timestamp)
		})
		It("backs up a table inheriting from multiple parents when only one parent is included", func() {
			skipIfOldBackupVersionBefore("1.29.0") // Inheritance behavior changed in this version
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.parent_a(a int);`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.parent_a`)
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.parent_b(b int);`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.parent_b`)
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.child() INHERITS (public.parent_a, public.parent_b);`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.child`)
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public.unrelated(c int);`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.unrelated`)

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir,
				"--include-table", `public.parent_a`,
				"--metadata-only")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir)

			assertTablesRestored(restoreConn, []string{"public.parent_a", "public.parent_b", "public.child"})
			assertTablesNotRestored(restoreConn, []string{"public.unrelated"})
			assertArtifactsCleaned(timestamp)
		})
		It("gpbackup with --include-table does not backup protocols and functions", func() {
			testhelper.AssertQueryRuns(backupConn,
				`CREATE TABLE t1(i int)`)
			defer testhelper.AssertQueryRuns(backupConn,
				`DROP TABLE public.t1`)
			testhelper.AssertQueryRuns(backupConn,
				`CREATE FUNCTION f1() RETURNS int AS 'SELECT 1' LANGUAGE SQL;`)
			defer testhelper.AssertQueryRuns(backupConn,
				`DROP FUNCTION public.f1()`)
			testhelper.AssertQueryRuns(backupConn,
				`CREATE PROTOCOL p1 (readfunc = f1);`)
			defer testhelper.AssertQueryRuns(backupConn,
				`DROP PROTOCOL p1`)

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir, "--include-table", "public.t1")
			timestamp := getBackupTimestamp(string(output))

			metadataFileContents := getMetdataFileContents(backupDir, timestamp, "metadata.sql")
			Expect(string(metadataFileContents)).To(ContainSubstring("t1"))
			Expect(string(metadataFileContents)).ToNot(ContainSubstring("public.f1()"))
			Expect(string(metadataFileContents)).ToNot(ContainSubstring("read_from_s3"))
			Expect(string(metadataFileContents)).ToNot(ContainSubstring("s3_protocol"))
		})
	})
	Describe("Restore include filtering", func() {
		It("runs gpbackup and gprestore with include-schema restore flag", func() {
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--include-schema", "schema2")

			assertRelationsCreated(restoreConn, 17)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with include-schema-file restore flag", func() {
			includeFile := iohelper.MustOpenFileForWriting("/tmp/include-schema.txt")
			utils.MustPrintln(includeFile, "schema2")
			utils.MustPrintln(includeFile, "public")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--include-schema-file", "/tmp/include-schema.txt")

			assertRelationsCreated(restoreConn, 37)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with include-table restore flag", func() {
			output := gpbackup(gpbackupPath, backupHelperPath)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "public.foo",
				"--include-table", "public.sales",
				"--include-table", "public.myseq1",
				"--include-table", "public.myview1")

			assertRelationsCreated(restoreConn, 16)
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 13, "public.foo": 40000})
		})
		It("runs gpbackup and gprestore with include-table-file restore flag", func() {
			includeFile := iohelper.MustOpenFileForWriting("/tmp/include-tables.txt")
			utils.MustPrintln(includeFile,
				"public.sales\npublic.foo\npublic.myseq1\npublic.myview1")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--include-table-file", "/tmp/include-tables.txt")

			assertRelationsCreated(restoreConn, 16)
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 13, "public.foo": 40000})

			_ = os.Remove("/tmp/include-tables.txt")
		})
		It("runs gpbackup and gprestore with include-table restore flag against a leaf partition", func() {
			skipIfOldBackupVersionBefore("1.7.2")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--leaf-partition-data")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--include-table", "public.sales_1_prt_jan17")

			assertRelationsCreated(restoreConn, 13)
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 1, "public.sales_1_prt_jan17": 1})
		})
		It("runs gpbackup and gprestore with include-table restore flag which implicitly filters schema restore list", func() {
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--include-table", "schema2.foo3")
			assertRelationsCreated(restoreConn, 1)
			assertDataRestored(restoreConn, map[string]int{"schema2.foo3": 100})
		})
		It("runs gprestore with --include-table flag to only restore tables specified ", func() {
			testhelper.AssertQueryRuns(backupConn,
				"CREATE TABLE public.table_to_include_with_stats(i int)")
			defer testhelper.AssertQueryRuns(backupConn,
				"DROP TABLE public.table_to_include_with_stats")
			testhelper.AssertQueryRuns(backupConn,
				"INSERT INTO public.table_to_include_with_stats VALUES (1)")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--include-table", "public.table_to_include_with_stats")

			assertRelationsCreated(restoreConn, 1)

			localSchemaTupleCounts := map[string]int{
				`public."table_to_include_with_stats"`: 1,
			}
			assertDataRestored(restoreConn, localSchemaTupleCounts)
			assertArtifactsCleaned(timestamp)
		})

	})
	Describe("Backup exclude filtering", func() {
		It("runs gpbackup and gprestore with exclude-schema backup flag", func() {
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--exclude-schema", "public")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, 17)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-schema-file backup flag", func() {
			skipIfOldBackupVersionBefore("1.17.0")
			excludeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-schema.txt")
			utils.MustPrintln(excludeFile, "public")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--exclude-schema-file", "/tmp/exclude-schema.txt")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, 17)
			assertDataRestored(restoreConn, schema2TupleCounts)

			_ = os.Remove("/tmp/exclude-schema.txt")
		})
		It("runs gpbackup and gprestore with exclude-table backup flag", func() {
			skipIfOldBackupVersionBefore("1.4.0")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--exclude-table", "schema2.foo2",
				"--exclude-table", "schema2.returns",
				"--exclude-table", "public.myseq2",
				"--exclude-table", "public.myview2")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS_AFTER_EXCLUDE)
			assertDataRestored(restoreConn, map[string]int{
				"schema2.foo3": 100,
				"public.foo":   40000,
				"public.holds": 50000,
				"public.sales": 13})
		})
		It("runs gpbackup and gprestore with exclude-table-file backup flag", func() {
			skipIfOldBackupVersionBefore("1.4.0")
			excludeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-tables.txt")
			utils.MustPrintln(excludeFile,
				"schema2.foo2\nschema2.returns\npublic.sales\npublic.myseq2\npublic.myview2")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--exclude-table-file", "/tmp/exclude-tables.txt")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, 8)
			assertDataRestored(restoreConn, map[string]int{
				"schema2.foo3": 100,
				"public.foo":   40000,
				"public.holds": 50000})

			_ = os.Remove("/tmp/exclude-tables.txt")
		})
		It("gpbackup with --exclude-table and then runs gprestore when functions and tables depending on each other", func() {
			skipIfOldBackupVersionBefore("1.19.0")

			testhelper.AssertQueryRuns(backupConn, "CREATE TABLE to_use_for_function (n int);")
			defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE to_use_for_function;")

			testhelper.AssertQueryRuns(backupConn, "INSERT INTO  to_use_for_function values (1);")
			testhelper.AssertQueryRuns(backupConn, "CREATE FUNCTION func1(val integer) RETURNS integer AS $$ BEGIN RETURN val + (SELECT n FROM to_use_for_function); END; $$ LANGUAGE PLPGSQL;;")
			defer testhelper.AssertQueryRuns(backupConn, "DROP FUNCTION func1(val integer);")

			testhelper.AssertQueryRuns(backupConn, "CREATE TABLE test_depends_on_function (id integer, claim_id character varying(20) DEFAULT ('WC-'::text || func1(10)::text)) DISTRIBUTED BY (id);")
			defer testhelper.AssertQueryRuns(backupConn, "DROP TABLE test_depends_on_function;")
			testhelper.AssertQueryRuns(backupConn, "INSERT INTO  test_depends_on_function values (1);")

			output := gpbackup(gpbackupPath, backupHelperPath,
				"--exclude-table", "public.holds")
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb")

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS-1+2) // -1 for exclude +2 for 2 new tables
			assertDataRestored(restoreConn, map[string]int{
				"public.foo":                      40000,
				"public.sales":                    13,
				"public.to_use_for_function":      1,
				"public.test_depends_on_function": 1})
			assertArtifactsCleaned(timestamp)
		})
	})
	Describe("Restore exclude filtering", func() {
		It("runs gpbackup and gprestore with exclude-schema restore flag", func() {
			output := gpbackup(gpbackupPath, backupHelperPath)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--exclude-schema", "public")

			assertRelationsCreated(restoreConn, 17)
			assertDataRestored(restoreConn, schema2TupleCounts)
		})
		It("runs gpbackup and gprestore with exclude-schema-file restore flag", func() {
			includeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-schema.txt")
			utils.MustPrintln(includeFile, "public")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--backup-dir", backupDir,
				"--redirect-db", "restoredb",
				"--exclude-schema-file", "/tmp/exclude-schema.txt")

			assertRelationsCreated(restoreConn, 17)
			assertDataRestored(restoreConn, schema2TupleCounts)

			_ = os.Remove("/tmp/exclude-schema.txt")
		})
		It("runs gpbackup and gprestore with exclude-table restore flag", func() {
			// Create keyword table to make sure we properly escape it during the exclusion check.
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public."user"(i int)`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public."user"`)

			output := gpbackup(gpbackupPath, backupHelperPath)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--exclude-table", "schema2.foo2",
				"--exclude-table", "schema2.returns",
				"--exclude-table", "public.myseq2",
				"--exclude-table", "public.myview2",
				"--exclude-table", "public.user")

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS_AFTER_EXCLUDE)
			assertDataRestored(restoreConn, map[string]int{
				"schema2.foo3": 100, "public.foo": 40000, "public.holds": 50000, "public.sales": 13})
		})
		It("runs gpbackup and gprestore with exclude-table-file restore flag", func() {
			// Create keyword table to make sure we properly escape it during the exclusion check.
			testhelper.AssertQueryRuns(backupConn, `CREATE TABLE public."user"(i int)`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public."user"`)

			includeFile := iohelper.MustOpenFileForWriting("/tmp/exclude-tables.txt")
			utils.MustPrintln(includeFile,
				"schema2.foo2\nschema2.returns\npublic.myseq2\npublic.myview2\npublic.user")
			output := gpbackup(gpbackupPath, backupHelperPath,
				"--backup-dir", backupDir)
			timestamp := getBackupTimestamp(string(output))

			gprestore(gprestorePath, restoreHelperPath, timestamp,
				"--redirect-db", "restoredb",
				"--backup-dir", backupDir,
				"--exclude-table-file", "/tmp/exclude-tables.txt")

			assertRelationsCreated(restoreConn, TOTAL_RELATIONS_AFTER_EXCLUDE)
			assertDataRestored(restoreConn, map[string]int{
				"public.sales": 13,
				"public.foo":   40000})

			_ = os.Remove("/tmp/exclude-tables.txt")
		})
	})
	Describe("Exclude subpartitions for given root partition in leaf-partition-data mode", func() {
		It("runs gpbackup and gprestore with leaf-partition-data and exclude-table root partition backup flags", func() {
			testhelper.AssertQueryRuns(backupConn,
				`CREATE TABLE public.p1_sales (id int, a int, b int, region text)
				WITH (appendoptimized=true)
				DISTRIBUTED BY (id)
				PARTITION BY RANGE (a)
					SUBPARTITION BY RANGE (b)
					SUBPARTITION TEMPLATE (
						START (1) END (3) EVERY (1))
						SUBPARTITION BY LIST (region)
							SUBPARTITION TEMPLATE (
							SUBPARTITION usa VALUES ('usa'),
							SUBPARTITION europe VALUES ('europe'))
				( START (1) END (3) EVERY (1))`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.p1_sales CASCADE`)
			output := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--exclude-table", "public.p1_sales")
			timestamp := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")
			assertTablesNotRestored(restoreConn, []string{
				"public.p1_sales",
				"public.p1_sales_1_prt_1",
				"public.p1_sales_1_prt_1_2_prt_1",
				"public.p1_sales_1_prt_1_2_prt_1_3_prt_europe",
				"public.p1_sales_1_prt_1_2_prt_1_3_prt_usa",
				"public.p1_sales_1_prt_1_2_prt_2",
				"public.p1_sales_1_prt_1_2_prt_2_3_prt_europe",
				"public.p1_sales_1_prt_1_2_prt_2_3_prt_usa",
				"public.p1_sales_1_prt_2",
				"public.p1_sales_1_prt_2_2_prt_1",
				"public.p1_sales_1_prt_2_2_prt_1_3_prt_europe",
				"public.p1_sales_1_prt_2_2_prt_1_3_prt_usa",
				"public.p1_sales_1_prt_2_2_prt_2",
				"public.p1_sales_1_prt_2_2_prt_2_3_prt_europe",
				"public.p1_sales_1_prt_2_2_prt_2_3_prt_usa",
			})
		})
		It("runs gpbackup and gprestore with leaf-partition-data and exclude-table leaf partition backup flags", func() {
			testhelper.AssertQueryRuns(backupConn,
				`CREATE TABLE public.p2_sales (id int, a int, b int, region text)
				WITH (appendoptimized=true)
				DISTRIBUTED BY (id)
				PARTITION BY RANGE (a)
					SUBPARTITION BY RANGE (b)
					SUBPARTITION TEMPLATE (
						START (1) END (3) EVERY (1))
						SUBPARTITION BY LIST (region)
							SUBPARTITION TEMPLATE (
							SUBPARTITION usa VALUES ('usa'),
							SUBPARTITION europe VALUES ('europe'))
				( START (1) END (3) EVERY (1))`)
			testhelper.AssertQueryRuns(backupConn,
				`INSERT INTO public.p2_sales VALUES
					(1, 1, 1, 'usa'),
					(1, 1, 1, 'europe'),
					(1, 1, 2, 'usa'),
					(1, 1, 2, 'europe'),
					(1, 2, 1, 'usa'),
					(1, 2, 1, 'europe'),
					(1, 2, 2, 'usa'),
					(1, 2, 2, 'europe')
				`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.p2_sales CASCADE`)
			output := gpbackup(gpbackupPath, backupHelperPath, "--leaf-partition-data", "--exclude-table", "public.p2_sales_1_prt_1_2_prt_1_3_prt_usa")
			timestamp := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")
			assertTablesRestored(restoreConn, []string{
				"public.p2_sales",
				"public.p2_sales_1_prt_1",
				"public.p2_sales_1_prt_1_2_prt_1",
				"public.p2_sales_1_prt_1_2_prt_1_3_prt_europe",
				"public.p2_sales_1_prt_1_2_prt_2",
				"public.p2_sales_1_prt_1_2_prt_2_3_prt_europe",
				"public.p2_sales_1_prt_1_2_prt_2_3_prt_usa",
				"public.p2_sales_1_prt_2",
				"public.p2_sales_1_prt_2_2_prt_1",
				"public.p2_sales_1_prt_2_2_prt_1_3_prt_europe",
				"public.p2_sales_1_prt_2_2_prt_1_3_prt_usa",
				"public.p2_sales_1_prt_2_2_prt_2",
				"public.p2_sales_1_prt_2_2_prt_2_3_prt_europe",
				"public.p2_sales_1_prt_2_2_prt_2_3_prt_usa",
			})
			assertDataRestored(restoreConn, map[string]int{
				"public.p2_sales":                              7,
				"public.p2_sales_1_prt_1":                      3,
				"public.p2_sales_1_prt_1_2_prt_1":              1,
				"public.p2_sales_1_prt_1_2_prt_1_3_prt_europe": 1,
				"public.p2_sales_1_prt_1_2_prt_2":              2,
				"public.p2_sales_1_prt_1_2_prt_2_3_prt_europe": 1,
				"public.p2_sales_1_prt_1_2_prt_2_3_prt_usa":    1,
				"public.p2_sales_1_prt_2":                      4,
				"public.p2_sales_1_prt_2_2_prt_1":              2,
				"public.p2_sales_1_prt_2_2_prt_1_3_prt_europe": 1,
				"public.p2_sales_1_prt_2_2_prt_1_3_prt_usa":    1,
				"public.p2_sales_1_prt_2_2_prt_2":              2,
				"public.p2_sales_1_prt_2_2_prt_2_3_prt_europe": 1,
				"public.p2_sales_1_prt_2_2_prt_2_3_prt_usa":    1,
			})
			if backupConn.Version.Before("7") {
				assertTablesRestored(restoreConn, []string{
					"public.p2_sales_1_prt_1_2_prt_1_3_prt_usa",
				})
				assertDataRestored(restoreConn, map[string]int{
					"public.p2_sales_1_prt_1_2_prt_1_3_prt_usa": 0,
				})
			} else {
				assertTablesNotRestored(restoreConn, []string{
					"public.p2_sales_1_prt_1_2_prt_1_3_prt_usa",
				})
			}
		})
		It("runs gpbackup and gprestore without leaf-partition-data and with exclude-table root partition backup flags", func() {
			testhelper.AssertQueryRuns(backupConn,
				`CREATE TABLE public.p3_sales (id int, a int, b int, region text)
				WITH (appendoptimized=true)
				DISTRIBUTED BY (id)
				PARTITION BY RANGE (a)
					SUBPARTITION BY RANGE (b)
					SUBPARTITION TEMPLATE (
						START (1) END (3) EVERY (1))
						SUBPARTITION BY LIST (region)
							SUBPARTITION TEMPLATE (
							SUBPARTITION usa VALUES ('usa'),
							SUBPARTITION europe VALUES ('europe'))
				( START (1) END (3) EVERY (1))`)
			defer testhelper.AssertQueryRuns(backupConn, `DROP TABLE public.p3_sales CASCADE`)
			output := gpbackup(gpbackupPath, backupHelperPath, "--exclude-table", "public.p3_sales")
			timestamp := getBackupTimestamp(string(output))
			defer assertArtifactsCleaned(timestamp)
			gprestore(gprestorePath, restoreHelperPath, timestamp, "--redirect-db", "restoredb")
			assertTablesNotRestored(restoreConn, []string{
				"public.p3_sales",
				"public.p3_sales_1_prt_1",
				"public.p3_sales_1_prt_1_2_prt_1",
				"public.p3_sales_1_prt_1_2_prt_1_3_prt_europe",
				"public.p3_sales_1_prt_1_2_prt_1_3_prt_usa",
				"public.p3_sales_1_prt_1_2_prt_2",
				"public.p3_sales_1_prt_1_2_prt_2_3_prt_europe",
				"public.p3_sales_1_prt_1_2_prt_2_3_prt_usa",
				"public.p3_sales_1_prt_2",
				"public.p3_sales_1_prt_2_2_prt_1",
				"public.p3_sales_1_prt_2_2_prt_1_3_prt_europe",
				"public.p3_sales_1_prt_2_2_prt_1_3_prt_usa",
				"public.p3_sales_1_prt_2_2_prt_2",
				"public.p3_sales_1_prt_2_2_prt_2_3_prt_europe",
				"public.p3_sales_1_prt_2_2_prt_2_3_prt_usa",
			})
		})
	})
})
