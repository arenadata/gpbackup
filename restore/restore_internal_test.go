package restore

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/GreengageDB/gp-common-go-libs/cluster"
	"github.com/GreengageDB/gp-common-go-libs/dbconn"
	"github.com/GreengageDB/gp-common-go-libs/gplog"
	"github.com/GreengageDB/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/toc"
	"os"
	path "path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("restore internal tests", func() {
	statements := []toc.StatementWithType{
		{ // simple table
			Schema: "foo", Name: "bar", ObjectType: toc.OBJ_TABLE,
			Statement: "\n\nCREATE TABLE foo.bar (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // simple schema
			Schema: "foo", Name: "foo", ObjectType: toc.OBJ_SCHEMA,
			Statement: "\n\nCREATE SCHEMA foo;\n",
		},
		{ // table with a schema containing dots
			Schema: "\"foo.bar\"", Name: "baz", ObjectType: toc.OBJ_TABLE,
			Statement: "\n\nCREATE TABLE \"foo.bar\".baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // table with a schema containing quotes
			Schema: "\"foo\"bar\"", Name: "baz", ObjectType: toc.OBJ_TABLE,
			Statement: "\n\nCREATE TABLE \"foo\"bar\".baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // view with multiple schema replacements
			Schema: "foo", Name: "myview", ObjectType: toc.OBJ_VIEW,
			Statement: "\n\nCREATE VIEW foo.myview AS  SELECT bar.i\n   FROM foo.bar;\n",
		},
		{ // schema and table are the same name
			Schema: "foo", Name: "foo", ObjectType: toc.OBJ_TABLE,
			Statement: "\n\nCREATE TABLE foo.foo (\n\ti integer\n) DISTRIBUTED BY (i);\n",
		},
		{ // multi-line permissions block for a schema
			Schema: "foo", Name: "foo", ObjectType: toc.OBJ_SCHEMA,
			Statement: "\n\nREVOKE ALL ON SCHEMA foo FROM PUBLIC;\nGRANT ALL ON SCHEMA foo TO testuser;\n",
		},
		{ // multi-line permissions block for a view
			Schema: "foo", Name: "myview", ObjectType: toc.OBJ_VIEW,
			Statement: "\n\nREVOKE ALL ON TABLE foo.myview FROM PUBLIC;\nGRANT ALL ON TABLE foo.myview TO testuser;\n",
		},
		{ // multi-line permissions block for a non-schema object
			Schema: "foo", Name: "myfunc", ObjectType: toc.OBJ_FUNCTION,
			Statement: "\n\nREVOKE ALL ON FUNCTION foo.myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION foo.myfunc(integer) TO testuser;\n",
		},
		{ // multi-line permissions block with a schema containing dots
			Schema: "\"foo.bar\"", Name: "myfunc", ObjectType: toc.OBJ_FUNCTION,
			Statement: "\n\nREVOKE ALL ON FUNCTION \"foo.bar\".myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION \"foo.bar\".myfunc(integer) TO testuser;\n",
		},
		{ // ALTER TABLE ... ATTACH PARTITION statement
			Schema: "public", Name: "foopart_p1", ObjectType: toc.OBJ_TABLE, ReferenceObject: "public.foopart",
			Statement: "\n\nALTER TABLE public.foopart ATTACH PARTITION public.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
		},
		{ // ALTER TABLE ONLY ... ATTACH PARTITION statement
			Schema: "public", Name: "foopart_p1", ObjectType: toc.OBJ_TABLE, ReferenceObject: "public.foopart",
			Statement: "\n\nALTER TABLE ONLY public.foopart ATTACH PARTITION public.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
		},
		{
			Schema: "foo", Name: "bar", ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo.bar'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: "foo", Name: `"b'ar"`, ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo."b''ar"'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: "foo", Name: `"b.ar"`, ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo."b.ar"'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: `"fo.o"`, Name: "bar", ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = '"fo.o".bar'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: `"fo.o"`, Name: `"b'ar"`, ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = '"fo.o"."b''ar"'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: `"fo.o"`, Name: `"b.ar"`, ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = '"fo.o"."b.ar"'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: `"fo'o"`, Name: "bar", ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = '"fo''o".bar'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: `"fo'o"`, Name: `"b'ar"`, ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = '"fo''o"."b''ar"'::regclass::oid AND attname = 'i');`,
		},
		{
			Schema: `"fo'o"`, Name: `"b.ar"`, ObjectType: toc.OBJ_STATISTICS,
			Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = '"fo''o"."b.ar"'::regclass::oid AND attname = 'i');`,
		},
	}
	Describe("editStatementsRedirectStatements", func() {
		It("does not alter schemas if no redirect was specified", func() {
			originalStatements := make([]toc.StatementWithType, len(statements))
			copy(originalStatements, statements)

			editStatementsRedirectSchema(statements, "")

			// Loop through statements individually instead of comparing the whole arrays directly,
			// to make it easier to find the statements with issues
			for i := range statements {
				Expect(statements[i]).To(Equal(originalStatements[i]))
			}
		})
		It("changes schema in the sql statement", func() {
			// We need to temporarily set the version to 7 or later to test the ATTACH PARTITION replacement
			oldVersion := connectionPool.Version
			connectionPool.Version = dbconn.NewVersion("7.0.0")
			defer func() { connectionPool.Version = oldVersion }()

			editStatementsRedirectSchema(statements, "foo2")

			expectedStatements := []toc.StatementWithType{
				{
					Schema: "foo2", Name: "bar", ObjectType: toc.OBJ_TABLE,
					Statement: "\n\nCREATE TABLE foo2.bar (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "foo2", ObjectType: toc.OBJ_SCHEMA,
					Statement: "\n\nCREATE SCHEMA foo2;\n",
				},
				{
					Schema: "foo2", Name: "baz", ObjectType: toc.OBJ_TABLE,
					Statement: "\n\nCREATE TABLE foo2.baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "baz", ObjectType: toc.OBJ_TABLE,
					Statement: "\n\nCREATE TABLE foo2.baz (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "myview", ObjectType: toc.OBJ_VIEW,
					Statement: "\n\nCREATE VIEW foo2.myview AS  SELECT bar.i\n   FROM foo.bar;\n",
				},
				{
					Schema: "foo2", Name: "foo", ObjectType: toc.OBJ_TABLE,
					Statement: "\n\nCREATE TABLE foo2.foo (\n\ti integer\n) DISTRIBUTED BY (i);\n",
				},
				{
					Schema: "foo2", Name: "foo2", ObjectType: toc.OBJ_SCHEMA,
					Statement: "\n\nREVOKE ALL ON SCHEMA foo2 FROM PUBLIC;\nGRANT ALL ON SCHEMA foo2 TO testuser;\n",
				},
				{
					Schema: "foo2", Name: "myview", ObjectType: toc.OBJ_VIEW,
					Statement: "\n\nREVOKE ALL ON TABLE foo2.myview FROM PUBLIC;\nGRANT ALL ON TABLE foo2.myview TO testuser;\n",
				},
				{
					Schema: "foo2", Name: "myfunc", ObjectType: toc.OBJ_FUNCTION,
					Statement: "\n\nREVOKE ALL ON FUNCTION foo2.myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION foo2.myfunc(integer) TO testuser;\n",
				},
				{
					Schema: "foo2", Name: "myfunc", ObjectType: toc.OBJ_FUNCTION,
					Statement: "\n\nREVOKE ALL ON FUNCTION foo2.myfunc(integer) FROM PUBLIC;\nGRANT ALL ON FUNCTION foo2.myfunc(integer) TO testuser;\n",
				},
				{ // ALTER TABLE ... ATTACH PARTITION statement
					Schema: "foo2", Name: "foopart_p1", ObjectType: toc.OBJ_TABLE, ReferenceObject: "foo2.foopart",
					Statement: "\n\nALTER TABLE foo2.foopart ATTACH PARTITION foo2.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
				},
				{ // ALTER TABLE ONLY ... ATTACH PARTITION statement
					Schema: "foo2", Name: "foopart_p1", ObjectType: toc.OBJ_TABLE, ReferenceObject: "foo2.foopart",
					Statement: "\n\nALTER TABLE ONLY foo2.foopart ATTACH PARTITION foo2.foopart_p1 FOR VALUES FROM (0) TO (1);\n",
				},
				{
					Schema: "foo2", Name: "bar", ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2.bar'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: `"b'ar"`, ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2."b''ar"'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: `"b.ar"`, ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2."b.ar"'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: "bar", ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2.bar'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: `"b'ar"`, ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2."b''ar"'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: `"b.ar"`, ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2."b.ar"'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: "bar", ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2.bar'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: `"b'ar"`, ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2."b''ar"'::regclass::oid AND attname = 'i');`,
				},
				{
					Schema: "foo2", Name: `"b.ar"`, ObjectType: toc.OBJ_STATISTICS,
					Statement: `DELETE FROM pg_statistic WHERE (starelid, staattnum) =
(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'foo2."b.ar"'::regclass::oid AND attname = 'i');`,
				},
			}

			for i := range statements {
				//fmt.Println("\n\nACTUAL\n", statements[i], "\nEXPECTED\n", expectedStatements[i])
				Expect(statements[i]).To(Equal(expectedStatements[i]))
			}
		})
	})
	Describe("Restore statistic", Ordered, func() {
		var mock sqlmock.Sqlmock
		BeforeAll(func() {
			err := os.MkdirAll(path.Join(os.TempDir(), "backup/gpseg-1/backups/20250815/20250815120713/"), 0777)
			Expect(err).ToNot(HaveOccurred())

			err = os.WriteFile(path.Join(os.TempDir(), "backup/gpseg-1/backups/20250815/20250815120713/gpbackup_20250815120713_statistics.sql"), []byte(
				`UPDATE pg_class
SET
        relpages = 194::int,
        reltuples = 10000.000000::real
WHERE oid = 'public.t1'::regclass::oid;

DELETE FROM pg_statistic WHERE (starelid, staattnum) IN
        (SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'public.t1'::regclass::oid AND attname = 'a');
INSERT INTO pg_statistic SELECT
        attrelid,
        attnum,
        false::boolean,
        0.000000::real,
        8::integer,
        -1.000000::real,
        2::smallint,
        3::smallint,
        0::smallint,
        0::smallint,
        0::smallint,
        412::oid,
        412::oid,
        0::oid,
        0::oid,
        0::oid,
        NULL::real[],
        '{"0.313703984"}'::real[],
        NULL::real[],
        NULL::real[],
        NULL::real[],
        array_in('{"1","100",}', 'int8'::regtype::oid, -1),
        NULL,
        NULL,
        NULL,
        NULL
FROM pg_attribute WHERE attrelid = 'public.t1'::regclass::oid AND attname = 'a';
`), 0777)
			Expect(err).ToNot(HaveOccurred())

			err = os.WriteFile(path.Join(os.TempDir(), "backup/gpseg-1/backups/20250815/20250815120713/gpbackup_20250815120713_toc.yaml"), []byte(`
statisticsentries:
- schema: public
  name: t1
  objecttype: STATISTICS
  referenceobject: ""
  startbyte: 0
  endbyte: 129
  tier:
  - 0
  - 0
- schema: public
  name: t1
  objecttype: STATISTICS
  referenceobject: ""
  startbyte: 129
  endbyte: 300
  tier:
  - 0
  - 0
- schema: public
  name: t1
  objecttype: STATISTICS
  referenceobject: ""
  startbyte: 300
  endbyte: 967
  tier:
  - 0
  - 0
`), 0777)

			gplog.InitializeLogging("gprestore", path.Join(os.TempDir(), "gprestore_test"))
			gplog.SetLogFileVerbosity(gplog.LOGERROR)

			opts = &options.Options{}

			globalTOC = toc.NewTOC(path.Join(os.TempDir(), "/backup/gpseg-1/backups/20250815/20250815120713/gpbackup_20250815120713_toc.yaml"))
			globalTOC.InitializeMetadataEntryMap()

			DeferCleanup(func() {
				err := os.RemoveAll(path.Join(os.TempDir(), "backup"))
				Expect(err).NotTo(HaveOccurred())
			})
		})

		BeforeEach(func() {
			configCoordinator := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "gpseg-1"}
			testCluster := cluster.NewCluster([]cluster.SegConfig{configCoordinator})
			testFPInfo := filepath.NewFilePathInfo(testCluster, "", "20250815120713",
				"gpseg", false)
			testFPInfo.SegDirMap[-1] = path.Join(os.TempDir(), "/backup/gpseg-1")
			SetFPInfo(testFPInfo)

			connectionPool, mock, _, _, _ = testhelper.SetupTestEnvironment()
			connectionPool.Version = dbconn.NewVersion("6.0.0")
		})

		DescribeTable("Restore statistic with different backup versions", func(backupVersion string, needNestedLoop bool) {
			testConfig := history.BackupConfig{
				BackupVersion: backupVersion,
			}
			SetBackupConfig(&testConfig)

			if needNestedLoop {
				mock.ExpectExec("SET enable_nestloop = ON;").WillReturnResult(sqlmock.NewResult(0, 0))
			}
			mock.ExpectExec("UPDATE pg_class SET relpages = 194::int, reltuples = 10000\\.000000::real WHERE oid = 'public\\.t1'::regclass::oid;").
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("DELETE FROM pg_statistic WHERE \\(starelid, staattnum\\) IN" +
				" \\(SELECT attrelid, attnum FROM pg_attribute WHERE attrelid = 'public\\.t1'::regclass::oid AND attname = 'a'\\);").
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec("INSERT INTO pg_statistic SELECT" +
				" attrelid, attnum," +
				" false::boolean," +
				" 0\\.000000::real," +
				" 8::integer," +
				" -1.000000::real," +
				" 2::smallint, 3::smallint, 0::smallint, 0::smallint, 0::smallint," +
				" 412::oid, 412::oid, 0::oid, 0::oid, 0::oid," +
				" NULL::real\\[\\], '{\"0.313703984\"}'::real\\[\\], NULL::real\\[\\], NULL::real\\[\\], NULL::real\\[\\]," +
				" array_in\\('{\"1\",\"100\",}', 'int8'::regtype::oid, -1\\)," +
				" NULL, NULL, NULL, NULL FROM pg_attribute WHERE attrelid = 'public.t1'::regclass::oid AND attname = 'a';").
				WillReturnResult(sqlmock.NewResult(0, 1))
			restoreStatistics()
			err := mock.ExpectationsWereMet()
			Expect(err).NotTo(HaveOccurred())
		},
			Entry("before problematic version", "1.30.5_arenadata15", false),
			Entry("problematic version", "1.30.5_arenadata18", true),
			Entry("alter problematic version", "1.30.5_arenadata20", false),
		)
	})
})
