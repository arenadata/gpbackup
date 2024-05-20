package helper

import (
	"github.com/greenplum-db/gpbackup/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

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
})
