package helper

import (
	"bufio"
	"os"

	"github.com/greenplum-db/gpbackup/utils"
	"golang.org/x/sys/unix"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/pkg/errors"
)

type test_step struct {
	getRestorePipeWriter_arg_expect string
	getRestorePipeWriter_result     bool
	check_skip_file_arg_tableoid    int
	check_skip_file_result          bool
}

type MockHelperImpl struct {
	step_no        int
	expected_steps []test_step
}

func (h MockHelperImpl) getCurStep() test_step {
	return h.expected_steps[h.step_no]
}

var test_expected_scenario = []test_step{
	{},                                // placeholder as steps start from 1
	{"mock_100_2", true, -1, false},   // Can open pipe for table 100, check_skip_file shall not be called
	{"mock_200_3", true, -1, false},   // Can open pipe for table 200, check_skip_file shall not be called
	{"mock_200_4", false, 200, false}, // Can not open pipe for table 200, check_skip_file shall be called, skip file not exists
	{"mock_200_4", false, 200, true},  // Can not open pipe for table 200, check_skip_file shall called, skip file exists
	{"mock_200_5", true, -1, false},   // Went to the next batch, Can open pipe for table 200, check_skip_file shall not be called
}

func NewSkipFileTest() *MockHelperImpl {
	var ret MockHelperImpl
	ret.expected_steps = test_expected_scenario
	return &ret
}

func (h MockHelperImpl) getOidWithBatchListFromFile(oidFileName string) ([]oidWithBatch, error) {
	ret := []oidWithBatch{
		{100, 2},
		{200, 3},
		{200, 4},
		{200, 5},
	}
	return ret, nil
}

func (h MockHelperImpl) checkForSkipFile(pipeFile string, tableOid int) bool {

	step := h.getCurStep()
	Expect(tableOid).To(Equal(step.check_skip_file_arg_tableoid))
	ret := h.getCurStep().check_skip_file_result
	return ret
}

func (h MockHelperImpl) createPipe(pipe string) error {
	return nil
}
func (h *MockHelperImpl) getRestoreDataReader(fileToRead string, objToc *toc.SegmentTOC, oidList []int) (*RestoreReader, error) {
	return nil, errors.New("getRestoreDataReader Not implemented")
}

func (h *MockHelperImpl) getRestorePipeWriter(currentPipe string) (*bufio.Writer, *os.File, error) {

	h.step_no++
	Expect(currentPipe).To(Equal(h.expected_steps[h.step_no].getRestorePipeWriter_arg_expect))

	if test_expected_scenario[h.step_no].getRestorePipeWriter_result {
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

	Describe("doRestoreAgent", func() {
		It("Test skip file in doRestoreAgent", func() {
			Expect(1).To(Equal(1))

			save_singleDataFile := *singleDataFile
			save_content := *content
			save_oidFile := *oidFile
			save_isResizeRestore := *isResizeRestore
			save_origSize := *origSize
			save_destSize := *destSize
			save_pipeFile := *pipeFile
			save_onErrorContinue := *onErrorContinue

			*singleDataFile = false
			*content = 1
			*oidFile = "testoid.dat"
			*isResizeRestore = true
			*origSize = 1
			*destSize = 1
			*pipeFile = "mock"
			*onErrorContinue = true

			helper := NewSkipFileTest()

			err := doRestoreAgent_internal(helper, helper)

			Expect(err).To(BeNil())

			*singleDataFile = save_singleDataFile
			*content = save_content
			*oidFile = save_oidFile
			*isResizeRestore = save_isResizeRestore
			*origSize = save_origSize
			*destSize = save_destSize
			*pipeFile = save_pipeFile
			*onErrorContinue = save_onErrorContinue

		})
	})
})
