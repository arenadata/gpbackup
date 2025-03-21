package backup_test

import (
	"database/sql"
	"fmt"
	"math"
	"sort"

	"github.com/GreengageDB/gp-common-go-libs/structmatcher"
	"github.com/GreengageDB/gp-common-go-libs/testhelper"
	"github.com/greenplum-db/gpbackup/backup"
	"github.com/greenplum-db/gpbackup/options"
	"github.com/greenplum-db/gpbackup/testutils"
	"github.com/greenplum-db/gpbackup/toc"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("backup/predata_relations tests", func() {
	BeforeEach(func() {
		tocfile, backupfile = testutils.InitializeTestTOC(buffer, "predata")
	})
	Describe("PrintIdentityColumns", func() {
		mySequence := backup.Relation{SchemaOid: 0, Oid: 10, Schema: "public", Name: "mytable_mycol_seq"}
		otherRandomSeq := backup.Relation{SchemaOid: 0, Oid: 40, Schema: "public", Name: "random_seq"}
		seqIdentity := backup.Sequence{Relation: mySequence, IsIdentity: true, OwningTableSchema: "public", OwningTableOid: "20", OwningTable: "public.mytable", OwningColumn: "public.mytable.mycol", UnqualifiedOwningColumn: "mycol", OwningColumnAttIdentity: "d", Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}
		seqRandom := backup.Sequence{Relation: otherRandomSeq, IsIdentity: false, OwningTableSchema: "public", OwningTableOid: "30", OwningTable: "public.another_table", OwningColumn: "public.another_table.another_column", UnqualifiedOwningColumn: "another_column", OwningColumnAttIdentity: "", Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}

		getSeqDefReplace := func() string {
			seqDefReplace := ""
			if connectionPool.Version.AtLeast("6") {
				seqDefReplace = `
	START WITH 0`
			}

			return seqDefReplace
		}

		It("prints identity column statements for identity sequences", func() {
			sequences := []backup.Sequence{seqIdentity, seqRandom}
			backup.PrintIdentityColumns(backupfile, tocfile, sequences)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "mytable_mycol_seq", toc.OBJ_SEQUENCE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`ALTER TABLE public.mytable
ALTER COLUMN mycol ADD GENERATED BY DEFAULT AS IDENTITY (
	SEQUENCE NAME public.mytable_mycol_seq%s
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5);`, getSeqDefReplace()))
		})
	})

	Describe("PrintCreateSequenceStatements", func() {
		baseSequence := backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "seq_name"}
		seqDefault := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}
		seqNegIncr := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: -1, MaxVal: -1, MinVal: math.MinInt64, CacheVal: 5, IsCycled: false, IsCalled: true}}
		seqMaxPos := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: 100, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}
		seqMinPos := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 10, CacheVal: 5, IsCycled: false, IsCalled: true}}
		seqMaxNeg := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: -1, MaxVal: -10, MinVal: math.MinInt64, CacheVal: 5, IsCycled: false, IsCalled: true}}
		seqMinNeg := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: -1, MaxVal: -1, MinVal: -100, CacheVal: 5, IsCycled: false, IsCalled: true}}
		seqCycle := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: true, IsCalled: true}}
		seqStart := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: false}}
		emptySequenceMetadataMap := backup.MetadataMap{}

		getSeqDefReplace := func() string {
			seqDefReplace := ""
			if connectionPool.Version.AtLeast("6") {
				seqDefReplace = `
	START WITH 0`
			}
			return seqDefReplace
		}

		getSeqType := func() string {
			seqType := ""
			if connectionPool.Version.AtLeast("7") {
				seqType = "bigint"
			}
			return seqType
		}

		It("can print a sequence with all default options", func() {
			seqDefault.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "seq_name", toc.OBJ_SEQUENCE)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()))
		})
		It("can print a decreasing sequence", func() {
			seqNegIncr.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqNegIncr}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY -1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()))
		})
		It("can print an increasing sequence with a maximum value", func() {
			seqMaxPos.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqMaxPos}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY 1
	MAXVALUE 100
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()))
		})
		It("can print an increasing sequence with a minimum value", func() {
			seqMinPos.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqMinPos}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY 1
	NO MAXVALUE
	MINVALUE 10
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()))
		})
		It("can print a decreasing sequence with a maximum value", func() {
			seqMaxNeg.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqMaxNeg}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY -1
	MAXVALUE -10
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()))
		})
		It("can print a decreasing sequence with a minimum value", func() {
			seqMinNeg.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqMinNeg}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY -1
	NO MAXVALUE
	MINVALUE -100
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()))
		})
		It("can print a sequence that cycles", func() {
			seqCycle.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqCycle}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5
	CYCLE;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()))
		})
		It("can print a sequence with a start value", func() {
			if connectionPool.Version.AtLeast("6") {
				seqStart.Definition.StartVal = 7
			}
			seqStart.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqStart}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `CREATE SEQUENCE public.seq_name
	START WITH 7
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, false);`)
		})
		It("escapes a sequence containing single quotes", func() {
			baseSequenceWithQuote := backup.Relation{SchemaOid: 0, Oid: 1, Schema: "public", Name: "seq_'name"}
			seqWithQuote := backup.Sequence{Relation: baseSequenceWithQuote, Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true, Type: getSeqType()}}
			sequences := []backup.Sequence{seqWithQuote}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_'name%s
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_''name', 7, true);`, getSeqDefReplace()))
		})
		It("can print a sequence with privileges, an owner, and a comment for version", func() {
			sequenceMetadataMap := testutils.DefaultMetadataMap(toc.OBJ_SEQUENCE, true, true, true, false)
			sequenceMetadata := sequenceMetadataMap[seqDefault.GetUniqueID()]
			sequenceMetadata.Privileges[0].Update = false
			sequenceMetadataMap[seqDefault.GetUniqueID()] = sequenceMetadata
			seqDefault.Definition.Type = getSeqType()
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, sequenceMetadataMap)

			keywordReplace := toc.OBJ_TABLE
			if connectionPool.Version.AtLeast("6") {
				keywordReplace = toc.OBJ_SEQUENCE
			}

			expectedEntries := []string{fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()),
				"COMMENT ON SEQUENCE public.seq_name IS 'This is a sequence comment.';",
				fmt.Sprintf("ALTER %s public.seq_name OWNER TO testrole;", keywordReplace),
				`REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
REVOKE ALL ON SEQUENCE public.seq_name FROM testrole;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole;`}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
		It("can print a sequence with privileges WITH GRANT OPTION", func() {
			seqDefault.Definition.Type = getSeqType()
			sequenceMetadata := backup.ObjectMetadata{Privileges: []backup.ACL{testutils.DefaultACLWithGrantWithout("testrole", toc.OBJ_SEQUENCE, "UPDATE")}}
			sequenceMetadataMap := backup.MetadataMap{seqDefault.GetUniqueID(): sequenceMetadata}
			sequences := []backup.Sequence{seqDefault}
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, sequenceMetadataMap)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY 1
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 7, true);`, getSeqDefReplace()),
				`REVOKE ALL ON SEQUENCE public.seq_name FROM PUBLIC;
GRANT SELECT,USAGE ON SEQUENCE public.seq_name TO testrole WITH GRANT OPTION;`)
		})
		It("prints data_type of the sequence", func() {
			if connectionPool.Version.Before("7") {
				Skip("Test only applicable to GPDB 7 and above")
			}
			seqSmallInt := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 10, Type: "smallint", Increment: 2, MaxVal: math.MaxInt16, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}
			seqInteger := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 10, Type: "integer", Increment: 2, MaxVal: math.MaxInt32, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}
			seqBigInt := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 10, Type: "bigint", Increment: 2, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}
			sequences := []backup.Sequence{seqSmallInt}
			sequences = append(sequences, seqInteger)
			sequences = append(sequences, seqBigInt)
			backup.PrintCreateSequenceStatements(backupfile, tocfile, sequences, emptySequenceMetadataMap)
			expectedStatements := []string{
				fmt.Sprintf(`CREATE SEQUENCE public.seq_name
	AS smallint%s
	INCREMENT BY 2
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 10, true);`, getSeqDefReplace()),

				fmt.Sprintf(`CREATE SEQUENCE public.seq_name
	AS integer%s
	INCREMENT BY 2
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 10, true);`, getSeqDefReplace()),

				fmt.Sprintf(`CREATE SEQUENCE public.seq_name%s
	INCREMENT BY 2
	NO MAXVALUE
	NO MINVALUE
	CACHE 5;

SELECT pg_catalog.setval('public.seq_name', 10, true);`, getSeqDefReplace()),
			}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedStatements...)
		})
	})
	Describe("PrintCreateViewStatement", func() {
		var (
			view          backup.View
			emptyMetadata backup.ObjectMetadata
		)
		BeforeEach(func() {
			view = backup.View{Oid: 1, Schema: "shamwow", Name: "shazam", Definition: sql.NullString{String: "SELECT count(*) FROM pg_tables;", Valid: true}}
			emptyMetadata = backup.ObjectMetadata{}
		})
		It("can print a basic view", func() {
			backup.PrintCreateViewStatement(backupfile, tocfile, view, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "shamwow", "", "shazam", toc.OBJ_VIEW)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;`)
		})
		It("can print a view with privileges, an owner, a comment, and a security label", func() {
			hasSecurityLabel := false
			if connectionPool.Version.AtLeast("6") {
				hasSecurityLabel = true
			}

			viewMetadata := testutils.DefaultMetadata(toc.OBJ_VIEW, true, true, true, hasSecurityLabel)
			backup.PrintCreateViewStatement(backupfile, tocfile, view, viewMetadata)

			keywordReplace := toc.OBJ_TABLE
			if connectionPool.Version.AtLeast("6") {
				keywordReplace = toc.OBJ_VIEW
			}

			expectedEntries := []string{"CREATE VIEW shamwow.shazam AS SELECT count(*) FROM pg_tables;",
				"COMMENT ON VIEW shamwow.shazam IS 'This is a view comment.';",
				fmt.Sprintf("ALTER %s shamwow.shazam OWNER TO testrole;", keywordReplace),
				`REVOKE ALL ON TABLE shamwow.shazam FROM PUBLIC;
REVOKE ALL ON TABLE shamwow.shazam FROM testrole;
GRANT ALL ON TABLE shamwow.shazam TO testrole;`}

			if connectionPool.Version.AtLeast("6") {
				expectedEntries = append(expectedEntries, "SECURITY LABEL FOR dummy ON VIEW shamwow.shazam IS 'unclassified';")
			}

			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
		It("can print a view with options", func() {
			view.Options = " WITH (security_barrier=true)"
			backup.PrintCreateViewStatement(backupfile, tocfile, view, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "shamwow", "", "shazam", toc.OBJ_VIEW)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
				`CREATE VIEW shamwow.shazam WITH (security_barrier=true) AS SELECT count(*) FROM pg_tables;`)
		})
	})
	Describe("PrintAlterSequenceStatements", func() {
		baseSequence := backup.Relation{Schema: "public", Name: "seq_name"}
		seqDefault := backup.Sequence{Relation: baseSequence, Definition: backup.SequenceDefinition{LastVal: 7, Increment: 1, MaxVal: math.MaxInt64, MinVal: 1, CacheVal: 5, IsCycled: false, IsCalled: true}}
		It("prints nothing for a sequence without an owning column", func() {
			seqDefault.OwningColumn = ""
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, tocfile, sequences)
			Expect(tocfile.PredataEntries).To(BeEmpty())
			testhelper.NotExpectRegexp(buffer, `ALTER SEQUENCE`)
		})
		It("can print an ALTER SEQUENCE statement for a sequence with an owning column", func() {
			seqDefault.OwningColumn = "public.tablename.col_one"
			sequences := []backup.Sequence{seqDefault}
			backup.PrintAlterSequenceStatements(backupfile, tocfile, sequences)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "public", "", "seq_name", toc.OBJ_SEQUENCE_OWNER)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, `ALTER SEQUENCE public.seq_name OWNED BY public.tablename.col_one;`)
		})
	})
	Describe("SplitTablesByPartitionType", func() {
		var tables []backup.Table
		var includeList []options.Relation
		var expectedMetadataTables []backup.Table
		BeforeEach(func() {
			tables = []backup.Table{
				{
					Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 3, Schema: "public", Name: "part_parent1_inter1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "i"}},
				},
				{
					Relation:        backup.Relation{Oid: 4, Schema: "public", Name: "part_parent1_child1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 5, Schema: "public", Name: "part_parent1_child2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 6, Schema: "public", Name: "part_parent2_child1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 7, Schema: "public", Name: "part_parent2_child2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}},
				},
				{
					Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
				},
			}

			expectedMetadataTables = []backup.Table{
				{
					Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
				},
				{
					Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
					TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
				},
			}
		})
		Context("leafPartitionData and includeTables", func() {
			It("gets only parent partitions of included tables for metadata and only child partitions for data", func() {
				includeList = []options.Relation{
					{
						Oid:    1,
						Schema: "public",
						Name:   "part_parent1",
					},
					{
						Oid:    6,
						Schema: "public",
						Name:   "part_parent2_child1",
					},
					{
						Oid:    7,
						Schema: "public",
						Name:   "part_parent2_child2",
					},
					{
						Oid:    8,
						Schema: "public",
						Name:   "test_table",
					},
				}
				_ = cmdFlags.Set(options.LEAF_PARTITION_DATA, "true")

				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				// In GPDB 7+, leaf partitions are created and attached to the root in separate metadata DDL
				if connectionPool.Version.AtLeast("7") {
					expectedMetadataTables = tables
				}
				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1_child1", "public.part_parent1_child2", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(5))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("leafPartitionData only", func() {
			It("gets only parent partitions for metadata and only child partitions in data", func() {
				_ = cmdFlags.Set(options.LEAF_PARTITION_DATA, "true")
				includeList = []options.Relation{}
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				// In GPDB 7+, leaf partitions are created and attached to the root in separate metadata DDL
				if connectionPool.Version.AtLeast("7") {
					expectedMetadataTables = tables
				}
				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1_child1", "public.part_parent1_child2", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(5))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("includeTables only", func() {
			It("gets only parent partitions of included tables for metadata and only included tables for data", func() {
				_ = cmdFlags.Set(options.LEAF_PARTITION_DATA, "false")
				includeList = []options.Relation{
					{
						Oid:    1,
						Schema: "public",
						Name:   "part_parent1",
					},
					{
						Oid:    6,
						Schema: "public",
						Name:   "part_parent2_child1",
					},
					{
						Oid:    7,
						Schema: "public",
						Name:   "part_parent2_child2",
					},
					{
						Oid:    8,
						Schema: "public",
						Name:   "test_table",
					},
				}
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				// In GPDB 7+, leaf partitions are created and attached to the root in separate metadata DDL
				if connectionPool.Version.AtLeast("7") {
					expectedMetadataTables = tables
				}
				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1", "public.part_parent2_child1", "public.part_parent2_child2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(4))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
		})
		Context("neither leafPartitionData nor includeTables", func() {
			It("gets the same table list for both metadata and data", func() {
				includeList = []options.Relation{}
				tables = []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: "part_parent1"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: "part_parent2"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "p"}},
					},
					{
						Relation:        backup.Relation{Oid: 8, Schema: "public", Name: "test_table"},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "n"}},
					},
				}
				_ = cmdFlags.Set(options.LEAF_PARTITION_DATA, "false")
				_ = cmdFlags.Set(options.INCLUDE_RELATION, "")
				metadataTables, dataTables := backup.SplitTablesByPartitionType(tables, includeList)

				Expect(metadataTables).To(Equal(expectedMetadataTables))

				expectedDataTables := []string{"public.part_parent1", "public.part_parent2", "public.test_table"}
				dataTableNames := make([]string, 0)
				for _, table := range dataTables {
					dataTableNames = append(dataTableNames, table.FQN())
				}
				sort.Strings(dataTableNames)

				Expect(dataTables).To(HaveLen(3))
				Expect(dataTableNames).To(Equal(expectedDataTables))
			})
			It("adds a suffix to external partition tables", func() {
				tableShortName := "part_parent1_prt_1"
				tableLongName := "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"

				var expectedShortName string
				var expectedLongName string
				if connectionPool.Version.Before("7") {
					expectedShortName = "part_parent1_prt_1_ext_part_"
					expectedLongName = "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"
				} else {
					expectedShortName = tableShortName
					expectedLongName = tableLongName
				}

				includeList = []options.Relation{}
				tables = []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: tableShortName},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: tableLongName},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
				}
				_ = cmdFlags.Set(options.LEAF_PARTITION_DATA, "false")
				_ = cmdFlags.Set(options.INCLUDE_RELATION, "")
				metadataTables, _ := backup.SplitTablesByPartitionType(tables, includeList)

				expectedTables := []backup.Table{
					{
						Relation:        backup.Relation{Oid: 1, Schema: "public", Name: expectedShortName},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
					{
						Relation:        backup.Relation{Oid: 2, Schema: "public", Name: expectedLongName},
						TableDefinition: backup.TableDefinition{PartitionLevelInfo: backup.PartitionLevelInfo{Level: "l"}, IsExternal: true},
					},
				}
				Expect(metadataTables).To(HaveLen(2))
				structmatcher.ExpectStructsToMatch(&expectedTables[0], &metadataTables[0])
				structmatcher.ExpectStructsToMatch(&expectedTables[1], &metadataTables[1])
			})
		})
	})
	Describe("AppendExtPartSuffix", func() {
		It("adds a suffix to an unquoted external partition table", func() {
			tablename := "name"
			expectedName := "name_ext_part_"
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to an unquoted external partition table that is too long", func() {
			tablename := "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"
			expectedName := "long_naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to a quoted external partition table", func() {
			tablename := `"!name"`
			expectedName := `"!name_ext_part_"`
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
		It("adds a suffix to a quoted external partition table that is too long", func() {
			tablename := `"long!naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaame"`
			expectedName := `"long!naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_ext_part_"`
			suffixName := backup.AppendExtPartSuffix(tablename)
			Expect(suffixName).To(Equal(expectedName))
		})
	})
	Describe("PrintCreateMaterializedViewStatement", func() {
		var (
			mview         backup.View
			emptyMetadata backup.ObjectMetadata
		)
		BeforeEach(func() {
			if connectionPool.Version.Before("6.2.0") {
				Skip("Test only applicable to GPDB 6.2.0 and above")
			}
			mview = backup.View{Oid: 1, Schema: "schema1", Name: "mview1", Definition: sql.NullString{String: "SELECT count(*) FROM pg_tables;", Valid: true}, IsMaterialized: true, DistPolicy: backup.DistPolicy{Policy: "DISTRIBUTED BY (tablename)"}}
			emptyMetadata = backup.ObjectMetadata{}
		})
		It("can print a basic materialized view", func() {
			backup.PrintCreateViewStatement(backupfile, tocfile, mview, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "schema1", "", "mview1", toc.OBJ_MATERIALIZED_VIEW)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
				`CREATE MATERIALIZED VIEW schema1.mview1 AS SELECT count(*) FROM pg_tables
WITH NO DATA
DISTRIBUTED BY (tablename);`)
		})
		It("can print a materialized view with privileges, an owner, and a comment", func() {
			mviewMetadata := testutils.DefaultMetadata(toc.OBJ_MATERIALIZED_VIEW, true, true, true, false)
			backup.PrintCreateViewStatement(backupfile, tocfile, mview, mviewMetadata)
			expectedEntries := []string{`CREATE MATERIALIZED VIEW schema1.mview1 AS SELECT count(*) FROM pg_tables
WITH NO DATA
DISTRIBUTED BY (tablename);`,
				"COMMENT ON MATERIALIZED VIEW schema1.mview1 IS 'This is a materialized view comment.';",
				"ALTER MATERIALIZED VIEW schema1.mview1 OWNER TO testrole;",
				`REVOKE ALL ON TABLE schema1.mview1 FROM PUBLIC;
REVOKE ALL ON TABLE schema1.mview1 FROM testrole;
GRANT ALL ON TABLE schema1.mview1 TO testrole;`}
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer, expectedEntries...)
		})
		It("can print a materialized view with options and a tablespace", func() {
			mview.Options = " WITH (security_barrier=true)"
			mview.Tablespace = "myTablespace"
			backup.PrintCreateViewStatement(backupfile, tocfile, mview, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "schema1", "", "mview1", toc.OBJ_MATERIALIZED_VIEW)
			testutils.AssertBufferContents(tocfile.PredataEntries, buffer,
				`CREATE MATERIALIZED VIEW schema1.mview1 WITH (security_barrier=true) TABLESPACE myTablespace AS SELECT count(*) FROM pg_tables
WITH NO DATA
DISTRIBUTED BY (tablename);`)
		})
	})
	Describe("PrintCreateDummyViewStatement", func() {
		var emptyMetadata backup.ObjectMetadata
		BeforeEach(func() {
			emptyMetadata = backup.ObjectMetadata{}
		})
		It("can print a simple dummy view with 0 columns", func() {
			view := backup.View{
				Schema:     "schema1",
				Name:       "view1",
				ColumnDefs: []backup.ColumnDefinition{},
			}
			backup.PrintCreateDummyViewStatement(backupfile, tocfile, view, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "schema1", "", "view1", toc.OBJ_VIEW)
			Expect(string(buffer.Contents())).To(ContainSubstring("CREATE VIEW schema1.view1 AS"))
			Expect(string(buffer.Contents())).To(ContainSubstring("SELECT;"))
		})
		It("can print a simple dummy view with 1 columns", func() {
			view := backup.View{
				Schema: "schema1",
				Name:   "view1",
				ColumnDefs: []backup.ColumnDefinition{
					backup.ColumnDefinition{Type: "integer", Name: "i"},
				},
			}
			backup.PrintCreateDummyViewStatement(backupfile, tocfile, view, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "schema1", "", "view1", toc.OBJ_VIEW)
			Expect(string(buffer.Contents())).To(ContainSubstring("CREATE VIEW schema1.view1 AS"))
			Expect(string(buffer.Contents())).To(ContainSubstring("SELECT"))
			Expect(string(buffer.Contents())).To(ContainSubstring("NULL::integer AS i;"))
		})
		It("can print a simple dummy view with 2 columns", func() {
			view := backup.View{
				Schema: "schema1",
				Name:   "view1",
				ColumnDefs: []backup.ColumnDefinition{
					backup.ColumnDefinition{Type: "integer", Name: "i"},
					backup.ColumnDefinition{Type: "integer", Name: "j"},
				},
			}
			backup.PrintCreateDummyViewStatement(backupfile, tocfile, view, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "schema1", "", "view1", toc.OBJ_VIEW)
			Expect(string(buffer.Contents())).To(ContainSubstring("CREATE VIEW schema1.view1 AS"))
			Expect(string(buffer.Contents())).To(ContainSubstring("SELECT"))
			Expect(string(buffer.Contents())).To(ContainSubstring("NULL::integer AS i,"))
			Expect(string(buffer.Contents())).To(ContainSubstring("NULL::integer AS j;"))
		})
		It("can print a simple dummy view with a collation", func() {
			view := backup.View{
				Schema: "schema1",
				Name:   "view1",
				ColumnDefs: []backup.ColumnDefinition{
					backup.ColumnDefinition{Type: "integer", Name: "i"},
					backup.ColumnDefinition{Type: "integer", Name: "j", Collation: `pg_catalog."C"`},
				},
			}
			backup.PrintCreateDummyViewStatement(backupfile, tocfile, view, emptyMetadata)
			testutils.ExpectEntry(tocfile.PredataEntries, 0, "schema1", "", "view1", toc.OBJ_VIEW)
			Expect(string(buffer.Contents())).To(ContainSubstring("CREATE VIEW schema1.view1 AS"))
			Expect(string(buffer.Contents())).To(ContainSubstring("SELECT"))
			Expect(string(buffer.Contents())).To(ContainSubstring("NULL::integer AS i,"))
			Expect(string(buffer.Contents())).To(ContainSubstring(`NULL::integer COLLATE pg_catalog."C" AS j;`))
		})
	})
})
