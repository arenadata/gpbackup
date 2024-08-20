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

type RestoreReaderTestImpl struct{}

func (r *RestoreReaderTestImpl) waitForPlugin() error {
	// No plugins yet, no errors to detect
	return nil
}

func (r *RestoreReaderTestImpl) positionReader(pos uint64, oid int) error {
	return nil
}

func (r *RestoreReaderTestImpl) copyData(num int64) (int64, error) {
	return 0, errors.New("copyData Not implemented")
}

func (r *RestoreReaderTestImpl) copyAllData() (int64, error) {
	return 0, errors.New("copyAllData Not implemented")
}

func (r *RestoreReaderTestImpl) getFileHandle() *os.File {
	return nil
}

func (r *RestoreReaderTestImpl) getReaderType() ReaderType {
	return "nil"
}

type SkipFileTestStep struct {
	getRestorePipeWriter_arg_expect string
	getRestorePipeWriter_result     bool
	check_skip_file_arg_tableoid    int
	check_skip_file_result          bool
}

type RestoreMockHelperImpl struct {
	step_no           int
	expected_oidbatch []oidWithBatch
	expected_steps    []SkipFileTestStep

	opened_pipes_map map[string]string // Ginkgo matcher works over map value, will diplicate key here
	restoreData      *RestoreReaderTestImpl
}

func (h *RestoreMockHelperImpl) opened_pipes() []string {

	if h.opened_pipes_map == nil {
		h.opened_pipes_map = make(map[string]string)

		for k := range pipesMap {
			h.opened_pipes_map[k] = k
		}
	}
	ret := make([]string, 0, len(h.opened_pipes_map))
	for k := range h.opened_pipes_map {
		ret = append(ret, k)
	}
	return ret
}

func (h *RestoreMockHelperImpl) getCurStep() SkipFileTestStep {
	Expect(h.step_no).To(BeNumerically("<", len(h.expected_steps)))
	return h.expected_steps[h.step_no]
}

func NewSkipFileTest(batches []oidWithBatch, steps []SkipFileTestStep) *RestoreMockHelperImpl {
	var ret = new(RestoreMockHelperImpl)
	ret.expected_oidbatch = batches
	ret.expected_steps = steps
	ret.opened_pipes_map = nil
	ret.restoreData = nil

	return ret
}

func (h *RestoreMockHelperImpl) getOidWithBatchListFromFile(oidFileName string) ([]oidWithBatch, error) {
	return h.expected_oidbatch, nil
}

func (h *RestoreMockHelperImpl) checkForSkipFile(pipeFile string, tableOid int) bool {

	step := h.getCurStep()
	Expect(tableOid).To(Equal(step.check_skip_file_arg_tableoid))
	ret := h.getCurStep().check_skip_file_result
	return ret
}

func (h *RestoreMockHelperImpl) createPipe(pipe string) error {
	// Check that pipe was not opened yet
	Expect(h.opened_pipes()).ShouldNot(ContainElement(pipe))

	h.opened_pipes_map[pipe] = pipe
	return nil
}

func (h *RestoreMockHelperImpl) flushAndCloseRestoreWriter(pipeName string, oid int) error {

	// Check that we are closing pipe which is opened
	Expect(h.opened_pipes()).To(ContainElement(pipeName))
	delete(h.opened_pipes_map, pipeName)
	return nil
}

func (h *RestoreMockHelperImpl) getRestoreDataReader(fileToRead string, objToc *toc.SegmentTOC, oidList []int) (RestoreReader, error) {
	if h.restoreData != nil {
		return h.restoreData, nil
	}
	return nil, errors.New("getRestoreDataReader Not implemented")
}

func (h *RestoreMockHelperImpl) getRestorePipeWriter(currentPipe string) (*bufio.Writer, *os.File, error) {
	h.step_no++
	Expect(currentPipe).To(Equal(h.getCurStep().getRestorePipeWriter_arg_expect))

	// The pipe should be created before
	Expect(h.opened_pipes()).Should(ContainElement(currentPipe))

	if h.getCurStep().getRestorePipeWriter_result {
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
		var (
			save_singleDataFile  bool
			save_content         int
			save_oidFile         string
			save_isResizeRestore bool
			save_origSize        int
			save_destSize        int
			save_pipeFile        string
			save_onErrorContinue bool
		)

		BeforeEach(func() {
			save_singleDataFile = *singleDataFile
			save_content = *content
			save_oidFile = *oidFile
			save_isResizeRestore = *isResizeRestore
			save_origSize = *origSize
			save_destSize = *destSize
			save_pipeFile = *pipeFile
			save_onErrorContinue = *onErrorContinue

			*singleDataFile = false
			*content = 1
			*oidFile = "testoid.dat"
			*isResizeRestore = true
			*origSize = 5
			*destSize = 3
			*pipeFile = "mock"
			*onErrorContinue = true
		})

		AfterEach(func() {
			*singleDataFile = save_singleDataFile
			*content = save_content
			*oidFile = save_oidFile
			*isResizeRestore = save_isResizeRestore
			*origSize = save_origSize
			*destSize = save_destSize
			*pipeFile = save_pipeFile
			*onErrorContinue = save_onErrorContinue
		})
		It("successfully restores using a single data file when inputs are valid and no errors occur", func() {
			*singleDataFile = true

			oid_batch := []oidWithBatch{{oid: 1, batch: 1}}
			steps := []SkipFileTestStep{
				{},
				{getRestorePipeWriter_arg_expect: "mock_1_1", getRestorePipeWriter_result: true, check_skip_file_arg_tableoid: 1, check_skip_file_result: false},
			}

			mockHelper := NewSkipFileTest(oid_batch, steps)
			mockHelper.restoreData = &RestoreReaderTestImpl{}

			// Prepare the toc file
			testDir := "" //"/tmp/helper_test/20180101/20180101010101/"
			*tocFile = fmt.Sprintf("%stest_toc.yaml", testDir)
			dataLength := 128*1024 + 1
			// Write custom TOC
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
			fToc, _ := os.Create(*tocFile)
			_, _ = fToc.WriteString(customTOC)
			defer func() {
				_ = os.Remove(*tocFile)
			}()

			*dataFile = "test_data.dat"
			// Call the function under test
			err := doRestoreAgent_internal(mockHelper, mockHelper)

			Expect(err).ToNot(HaveOccurred())
		})
		It("successfully restores using multiple data files when inputs are valid and no errors occur", func() {
			// Call the function under test
			oid_batch := []oidWithBatch{{oid: 1, batch: 1}}
			steps := []SkipFileTestStep{
				{},
				{getRestorePipeWriter_arg_expect: "mock_1_1", getRestorePipeWriter_result: true, check_skip_file_arg_tableoid: 1, check_skip_file_result: false},
			}

			mockHelper := NewSkipFileTest(oid_batch, steps)
			mockHelper.restoreData = &RestoreReaderTestImpl{}

			err := doRestoreAgent_internal(mockHelper, mockHelper)

			Expect(err).ToNot(HaveOccurred())
		})
		It("skips batches with corresponding skip file in doRestoreAgent", func() {
			// Test Scenario 1. Simulate 1 pass for the doRestoreAgent() function with the specified oids, batches and expected calls
			oid_batch := []oidWithBatch{
				{100, 2},
				{200, 3},
				{200, 4},
				{200, 5},
			}

			expected_scenario := []SkipFileTestStep{
				{},                                // placeholder as steps start from 1
				{"mock_100_2", true, -1, false},   // Can open pipe for table 100, check_skip_file shall not be called
				{"mock_200_3", true, -1, false},   // Can open pipe for table 200, check_skip_file shall not be called
				{"mock_200_4", false, 200, false}, // Can not open pipe for table 200, check_skip_file shall be called, skip file not exists
				{"mock_200_4", false, 200, true},  // Can not open pipe for table 200, check_skip_file shall called, skip file exists
				{"mock_200_5", true, -1, false},   // Went to the next batch, Can open pipe for table 200, check_skip_file shall not be called
			}

			helper := NewSkipFileTest(oid_batch, expected_scenario)

			err := doRestoreAgent_internal(helper, helper)
			Expect(err).To(BeNil())
		})
	})
})
