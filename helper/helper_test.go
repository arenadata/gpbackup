package helper

import (
	"bufio"
	"fmt"
	"os"

	"github.com/greenplum-db/gpbackup/utils"
	"golang.org/x/sys/unix"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/pkg/errors"
)

var (
	testDir                 = "/tmp/helper_test/20180101/20180101010101"
	testTocFile             = fmt.Sprintf("%s/test_toc.yaml", testDir)
	examplePluginTestConfig = "/tmp/test_example_plugin_config.yaml"
)

type RestoreReaderTestImpl struct{}

func (r *RestoreReaderTestImpl) waitForPlugin() error {
	// No plugins yet, no errors to detect
	return nil
}

func (r *RestoreReaderTestImpl) positionReader(pos uint64, oid int) error {
	return nil
}

func (r *RestoreReaderTestImpl) copyData(num int64) (int64, error) {
	return 1, nil
}

func (r *RestoreReaderTestImpl) copyAllData() (int64, error) {
	return 1, nil
}

func (r *RestoreReaderTestImpl) closeFileHandle() {
}

func (r *RestoreReaderTestImpl) getReaderType() ReaderType {
	return "nil"
}

type SkipFileTestStep struct {
	getRestorePipeWriterArgExpect string
	getRestorePipeWriterResult    bool
	checkSkipFileArgTableOid      int
	checkSkipFileResult           bool
}

type RestoreMockHelperImpl struct {
	stepNo           int
	expectedOidBatch []oidWithBatch
	expectedSteps    []SkipFileTestStep

	openedPipesMap map[string]string // Ginkgo matcher works over map value, will diplicate key here
	restoreData    *RestoreReaderTestImpl
}

func (h *RestoreMockHelperImpl) openedPipes() []string {
	if h.openedPipesMap == nil {
		h.openedPipesMap = make(map[string]string)

		for k := range pipesMap {
			h.openedPipesMap[k] = k
		}
	}
	ret := make([]string, 0, len(h.openedPipesMap))
	for k := range h.openedPipesMap {
		ret = append(ret, k)
	}
	return ret
}

func (h *RestoreMockHelperImpl) getCurStep() SkipFileTestStep {
	Expect(h.stepNo).To(BeNumerically("<", len(h.expectedSteps)))
	return h.expectedSteps[h.stepNo]
}

func (h *RestoreMockHelperImpl) closeAndDeletePipe(tableOid int, batchNum int) {
}

func NewSkipFileTest(batches []oidWithBatch, steps []SkipFileTestStep) *RestoreMockHelperImpl {
	var ret = new(RestoreMockHelperImpl)
	ret.expectedOidBatch = batches
	ret.expectedSteps = steps
	ret.openedPipesMap = nil
	ret.restoreData = &RestoreReaderTestImpl{}

	return ret
}

func (h *RestoreMockHelperImpl) getOidWithBatchListFromFile(oidFileName string) ([]oidWithBatch, error) {
	return h.expectedOidBatch, nil
}

func (h *RestoreMockHelperImpl) checkForSkipFile(pipeFile string, tableOid int) bool {
	step := h.getCurStep()
	Expect(tableOid).To(Equal(step.checkSkipFileArgTableOid))
	ret := h.getCurStep().checkSkipFileResult
	return ret
}

func (h *RestoreMockHelperImpl) createPipe(pipe string) error {
	// Check that pipe was not opened yet
	Expect(h.openedPipes()).ShouldNot(ContainElement(pipe))

	h.openedPipesMap[pipe] = pipe
	return nil
}

func (h *RestoreMockHelperImpl) flushAndCloseRestoreWriter(pipeName string, oid int) error {
	// Check that we are closing pipe which is opened
	Expect(h.openedPipes()).To(ContainElement(pipeName))
	delete(h.openedPipesMap, pipeName)
	return nil
}

func (h *RestoreMockHelperImpl) getRestoreDataReader(fileToRead string, objToc *toc.SegmentTOC, oidList []int) (IRestoreReader, error) {
	if h.restoreData != nil {
		return h.restoreData, nil
	}
	return nil, errors.New("getRestoreDataReader Not implemented")
}

func (h *RestoreMockHelperImpl) getRestorePipeWriter(currentPipe string) (*bufio.Writer, *os.File, error) {
	h.stepNo++
	Expect(currentPipe).To(Equal(h.getCurStep().getRestorePipeWriterArgExpect))

	// The pipe should be created before
	Expect(h.openedPipes()).Should(ContainElement(currentPipe))

	if h.getCurStep().getRestorePipeWriterResult {
		var writer bufio.Writer
		return &writer, nil, nil
	}
	return nil, nil, unix.ENXIO
}

var _ = Describe("helper tests", func() {
	var pluginConfig utils.PluginConfig
	var isSubset bool
	var fileToRead, fileGzToRead, fileZstdToRead string

	InitializeGlobals()

	*isFiltered = true
	fileToRead = "/tmp/file"
	fileGzToRead = "/tmp/file.gz"
	fileZstdToRead = "/tmp/file.zst"
	pluginConfig = utils.PluginConfig{
		ExecutablePath: "/a/b/myPlugin",
		ConfigPath:     "/tmp/my_plugin_config.yaml",
		Options:        make(map[string]string),
	}

	BeforeEach(func() {
		err := os.MkdirAll(testDir, 0777)
		Expect(err).ShouldNot(HaveOccurred())
	})

	Describe("Check subset flag", func() {
		It("when restore_subset is off, --on-error-continue is false, compression is not used", func() {
			pluginConfig.Options["restore_subset"] = "off"
			*onErrorContinue = false
			isSubset = getSubsetFlag(fileToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is on, --on-error-continue is false, compression is not used", func() {
			pluginConfig.Options["restore_subset"] = "on"
			*onErrorContinue = false
			isSubset = getSubsetFlag(fileToRead, &pluginConfig)
			Expect(isSubset).To(Equal(true))
		})
		It("when restore_subset is off, --on-error-continue is true, compression is not used", func() {
			pluginConfig.Options["restore_subset"] = "off"
			*onErrorContinue = true
			isSubset = getSubsetFlag(fileToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is on, --on-error-continue is true, compression is not used", func() {
			pluginConfig.Options["restore_subset"] = "on"
			*onErrorContinue = true
			isSubset = getSubsetFlag(fileToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is off, --on-error-continue is false, compression \"gz\" is used", func() {
			pluginConfig.Options["restore_subset"] = "off"
			*onErrorContinue = false
			isSubset = getSubsetFlag(fileGzToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is on, --on-error-continue is false, compression \"gz\" is used", func() {
			pluginConfig.Options["restore_subset"] = "on"
			*onErrorContinue = false
			isSubset = getSubsetFlag(fileGzToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is off, --on-error-continue is true, compression \"gz\" is used", func() {
			pluginConfig.Options["restore_subset"] = "off"
			*onErrorContinue = true
			isSubset = getSubsetFlag(fileGzToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is on, --on-error-continue is true, compression \"gz\" is used", func() {
			pluginConfig.Options["restore_subset"] = "on"
			*onErrorContinue = true
			isSubset = getSubsetFlag(fileGzToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is off, --on-error-continue is false, compression \"zstd\" is used", func() {
			pluginConfig.Options["restore_subset"] = "off"
			*onErrorContinue = false
			isSubset = getSubsetFlag(fileZstdToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is on, --on-error-continue is false, compression \"zstd\" is used", func() {
			pluginConfig.Options["restore_subset"] = "on"
			*onErrorContinue = false
			isSubset = getSubsetFlag(fileZstdToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is off, --on-error-continue is true, compression \"zstd\" is used", func() {
			pluginConfig.Options["restore_subset"] = "off"
			*onErrorContinue = true
			isSubset = getSubsetFlag(fileZstdToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
		It("when restore_subset is on, --on-error-continue is true, compression \"zstd\" is used", func() {
			pluginConfig.Options["restore_subset"] = "on"
			*onErrorContinue = true
			isSubset = getSubsetFlag(fileZstdToRead, &pluginConfig)
			Expect(isSubset).To(Equal(false))
		})
	})

	Describe("doRestoreAgent Mocked unit tests", func() {
		BeforeEach(func() {
			// Setup mocked tests environment
			*singleDataFile = false
			*content = 1
			*oidFile = "testoid.dat"
			*isResizeRestore = true
			*origSize = 5
			*destSize = 3
			*pipeFile = "mock"
			*onErrorContinue = true
		})
		It("successfully restores using a single data file when inputs are valid and no errors occur", func() {
			*singleDataFile = true

			oidBatch := []oidWithBatch{{oid: 1, batch: 1}}
			steps := []SkipFileTestStep{
				{},
				{getRestorePipeWriterArgExpect: "mock_1_1", getRestorePipeWriterResult: true, checkSkipFileArgTableOid: 1, checkSkipFileResult: false},
			}

			mockHelper := NewSkipFileTest(oidBatch, steps)

			// Prepare and write the toc file
			testDir := "" //"/tmp/helper_test/20180101/20180101010101/"
			*tocFile = fmt.Sprintf("%stest_toc.yaml", testDir)
			writeTestTOC(*tocFile)
			defer func() {
				_ = os.Remove(*tocFile)
			}()

			*dataFile = "test_data.dat"
			// Call the function under test
			err := doRestoreAgentInternal(mockHelper, mockHelper)

			Expect(err).ToNot(HaveOccurred())
		})
		It("successfully restores using multiple data files when inputs are valid and no errors occur", func() {
			// Call the function under test
			oidBatch := []oidWithBatch{{oid: 1, batch: 1}}
			steps := []SkipFileTestStep{
				{},
				{getRestorePipeWriterArgExpect: "mock_1_1", getRestorePipeWriterResult: true, checkSkipFileArgTableOid: 1, checkSkipFileResult: false},
			}

			mockHelper := NewSkipFileTest(oidBatch, steps)

			err := doRestoreAgentInternal(mockHelper, mockHelper)

			Expect(err).ToNot(HaveOccurred())
		})
		It("skips batches with corresponding skip file in doRestoreAgent", func() {
			// Test Scenario 1. Simulate 1 pass for the doRestoreAgent() function with the specified oids, batches and expected calls
			oidBatch := []oidWithBatch{
				{100, 0},
				{200, 0},
				{200, 1},
				{200, 2},
			}

			expectedScenario := []SkipFileTestStep{
				{},                               // placeholder as steps start from 1
				{"mock_100_0", true, -1, false},  // Can open pipe for table 100, check_skip_file shall not be called
				{"mock_200_0", true, -1, false},  // Can open pipe for table 200, check_skip_file shall not be called
				{"mock_200_1", false, 200, true}, // Can not open pipe for table 200, check_skip_file shall called, skip file exists
				{"mock_200_2", true, -1, false},  // Went to the next batch, Can open pipe for table 200, check_skip_file shall not be called
			}

			helper := NewSkipFileTest(oidBatch, expectedScenario)

			err := doRestoreAgentInternal(helper, helper)
			Expect(err).To(BeNil())
		})
		It("skips batches if skip file is discovered with resize restore", func() {
			*isResizeRestore = true
			*origSize = 3
			*destSize = 5

			oidBatch := []oidWithBatch{
				{100, 0},
				{200, 0},
				{200, 1},
				{200, 2},
			}

			expectedScenario := []SkipFileTestStep{
				{},                               // placeholder as steps start from 1
				{"mock_100_0", true, -1, false},  // Can open pipe for table 100, check_skip_file shall not be called
				{"mock_200_0", true, -1, false},  // Can open pipe for table 200, check_skip_file shall not be called
				{"mock_200_1", false, 200, true}, // Can not open pipe for table 200, check_skip_file shall called, skip file exists
				{"mock_200_2", true, -1, false},  // Went to the next batch, Can open pipe for table 200, check_skip_file shall not be called
			}

			helper := NewSkipFileTest(oidBatch, expectedScenario)
			err := doRestoreAgentInternal(helper, helper)
			Expect(err).To(BeNil())

		})
		It("skips batches if skip file is discovered with single datafile", func() {
			*singleDataFile = true
			*isResizeRestore = false
			*tocFile = testTocFile

			// Although pure concept would be to mock TOC file as well, to keep things simpler
			// let's use real TOC file here
			writeTestTOC(testTocFile)
			defer func() {
				_ = os.Remove(*tocFile)
			}()

			oidBatch := []oidWithBatch{
				{100, 0},
				{200, 0},
				{200, 1},
				{200, 2},
			}

			expectedScenario := []SkipFileTestStep{
				{},                               // placeholder as steps start from 1
				{"mock_100_0", true, -1, false},  // Can open pipe for table 100, check_skip_file shall not be called
				{"mock_200_0", true, -1, false},  // Can open pipe for table 200, check_skip_file shall not be called
				{"mock_200_1", false, 200, true}, // Can not open pipe for table 200, check_skip_file shall called, skip file exists
				{"mock_200_2", true, -1, false},  // Went to the next batch, Can open pipe for table 200, check_skip_file shall not be called
			}

			helper := NewSkipFileTest(oidBatch, expectedScenario)
			err := doRestoreAgentInternal(helper, helper)
			Expect(err).To(BeNil())
		})
	})
})

func writeTestTOC(tocFile string) {
	// Write test TOC. We are not going to read data using it, so dataLength is a random number
	dataLength := 100
	customTOC := fmt.Sprintf(`dataentries:
1:
    startbyte: 0
    endbyte: 18
2:
    startbyte: 18
    endbyte: %[1]d
3:
    startbyte: %[1]d
    endbyte: %d
`, dataLength+18, dataLength+18+18)
	fToc, err := os.Create(tocFile)
	Expect(err).ShouldNot(HaveOccurred())
	defer fToc.Close()
	fToc.WriteString(customTOC)
}
