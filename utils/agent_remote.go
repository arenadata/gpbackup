package utils

import (
	"fmt"
	"io"
	path "path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/GreengageDB/gp-common-go-libs/cluster"
	"github.com/GreengageDB/gp-common-go-libs/gplog"
	"github.com/GreengageDB/gp-common-go-libs/iohelper"
	"github.com/GreengageDB/gp-common-go-libs/operating"
	"github.com/greenplum-db/gpbackup/filepath"
	"github.com/pkg/errors"
)

var helperMutex sync.Mutex
var AgentErr = errors.New("agent error")

/*
 * Functions to run commands on entire cluster during both backup and restore
 */

/*
 * The reason that gprestore is in charge of creating the first pipe to ensure
 * that the first pipe is created before the first COPY FROM is issued.  If
 * gpbackup_helper was in charge of creating the first pipe, there is a
 * possibility that the COPY FROM commands start before gpbackup_helper is done
 * starting up and setting up the first pipe.
 */
func CreateSegmentPipeOnAllHostsForBackup(oid string, c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Creating segment data pipes", cluster.ON_SEGMENTS, func(contentID int) string {
		pipeName := fpInfo.GetSegmentPipeFilePath(contentID)
		pipeName = fmt.Sprintf("%s_%s", pipeName, oid)
		gplog.Debug("Creating pipe %s", pipeName)
		return fmt.Sprintf("mkfifo -m 0700 %s", pipeName)
	})
	c.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func CreateSegmentPipeOnAllHostsForRestore(oid string, c *cluster.Cluster, fpInfo filepath.FilePathInfo, helperIdx ...int) {
	oidWithBatch := strings.Split(oid, ",")
	remoteOutput := c.GenerateAndExecuteCommand("Creating segment data pipes", cluster.ON_SEGMENTS, func(contentID int) string {
		pipeName := fpInfo.GetSegmentPipeFilePath(contentID, helperIdx...)
		pipeName = fmt.Sprintf("%s_%s_%s", pipeName, oidWithBatch[0], oidWithBatch[1])
		gplog.Debug("Creating pipe %s", pipeName)
		return fmt.Sprintf("mkfifo %s", pipeName)
	})
	c.CheckClusterError(remoteOutput, "Unable to create segment data pipes", func(contentID int) string {
		return "Unable to create segment data pipe"
	})
}

func WriteOidListToSegments(oidList []string, c *cluster.Cluster, fpInfo filepath.FilePathInfo, helperIdx ...int) {
	fileSuffix := filepath.FormatSuffix("oid", helperIdx...)
	rsync_exists := CommandExists("rsync")
	if !rsync_exists {
		gplog.Fatal(errors.New("Failed to find rsync on PATH. Please ensure rsync is installed."), "")
	}

	localOidFile, err := operating.System.TempFile("", "gpbackup-oids")
	gplog.FatalOnError(err, "Cannot open temporary file to write oids")
	defer func() {
		err = operating.System.Remove(localOidFile.Name())
		if err != nil {
			gplog.Warn("Cannot remove temporary oid file: %s, Err: %s", localOidFile.Name(), err.Error())
		}
	}()

	WriteOidsToFile(localOidFile.Name(), oidList)

	generateScpCmd := func(contentID int) string {
		sourceFile := localOidFile.Name()
		hostname := c.GetHostForContent(contentID)
		dest := fpInfo.GetSegmentHelperFilePath(contentID, fileSuffix)

		return fmt.Sprintf(`rsync -e ssh %s %s:%s`, sourceFile, hostname, dest)
	}
	remoteOutput := c.GenerateAndExecuteCommand("rsync oid file to segments", cluster.ON_LOCAL|cluster.ON_SEGMENTS, generateScpCmd)

	errMsg := "Failed to rsync oid file"
	errFunc := func(contentID int) string {
		return "Failed to run rsync"
	}
	c.CheckClusterError(remoteOutput, errMsg, errFunc, false)
}

func WriteOidsToFile(filename string, oidList []string) {
	oidFp, err := iohelper.OpenFileForWriting(filename)
	gplog.FatalOnError(err, filename)
	defer func() {
		err = oidFp.Close()
		gplog.FatalOnError(err, filename)
	}()

	err = WriteOids(oidFp, oidList)
	gplog.FatalOnError(err, filename)
}

func WriteOids(writer io.Writer, oidList []string) error {
	var err error
	for _, oid := range oidList {
		_, err = writer.Write([]byte(oid + "\n"))
		if err != nil {
			// error logging handled in calling functions
			return err
		}
	}

	return nil
}

func VerifyHelperVersionOnSegments(version string, c *cluster.Cluster) {
	remoteOutput := c.GenerateAndExecuteCommand("Verifying gpbackup_helper version", cluster.ON_HOSTS, func(contentID int) string {
		gphome := operating.System.Getenv("GPHOME")
		return fmt.Sprintf("%s/bin/gpbackup_helper --version", gphome)
	})
	c.CheckClusterError(remoteOutput, "Could not verify gpbackup_helper version", func(contentID int) string {
		return "Could not verify gpbackup_helper version"
	})

	numIncorrect := 0
	for contentID, cmd := range remoteOutput.Commands {
		parsedSegVersion := ""
		segVersion := strings.TrimSpace(cmd.Stdout) // Expected format is "gpbackup_helper version [version string]"
		splitSegVersion := strings.Split(segVersion, " ")
		if len(splitSegVersion) == 3 {
			// Array access placed inside a length guard to keep error messages nice, instead of
			// spilling panics to the log
			parsedSegVersion = splitSegVersion[2]
		}
		if parsedSegVersion != version {
			gplog.Verbose("Version mismatch for gpbackup_helper on segment %d on host %s: Expected version %s, found version %s.", contentID, c.GetHostForContent(contentID), version, parsedSegVersion)
			numIncorrect++
		}
	}
	if numIncorrect > 0 {
		cluster.LogFatalClusterError("The version of gpbackup_helper must match the version of gpbackup/gprestore, but found gpbackup_helper binaries with invalid version", cluster.ON_HOSTS, numIncorrect)
	}
}

func StartGpbackupHelpers(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string, pluginConfigFile string, compressStr string, onErrorContinue bool, isFilter bool, wasTerminated *atomic.Bool, copyQueue int, isSingleDataFile bool, resizeCluster bool, origSize int, destSize int, verbosity int, helperIdx ...int) {
	// A mutex lock for cleaning up and starting gpbackup helpers prevents a
	// race condition that causes gpbackup_helpers to be orphaned if
	// gpbackup_helper cleanup happens before they are started.
	helperMutex.Lock()
	if wasTerminated.Load() {
		helperMutex.Unlock()
		select {} // Pause forever and wait for cleanup to exit program.
	}
	defer helperMutex.Unlock()

	gphomePath := operating.System.Getenv("GPHOME")
	pluginStr := ""
	if pluginConfigFile != "" {
		_, configFilename := path.Split(pluginConfigFile)
		pluginStr = fmt.Sprintf(" --plugin-config /tmp/%s", configFilename)
	}
	onErrorContinueStr := ""
	if onErrorContinue {
		onErrorContinueStr = " --on-error-continue"
	}
	filterStr := ""
	if isFilter {
		filterStr = " --with-filters"
	}
	singleDataFileStr := ""
	if isSingleDataFile {
		singleDataFileStr = " --single-data-file"
	}
	resizeStr := ""
	if resizeCluster {
		resizeStr = fmt.Sprintf(" --resize-cluster --orig-seg-count %d --dest-seg-count %d", origSize, destSize)
	}
	remoteOutput := c.GenerateAndExecuteCommand("Starting gpbackup_helper agent", cluster.ON_SEGMENTS, func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		oidFile := fpInfo.GetSegmentHelperFilePath(contentID, filepath.FormatSuffix("oid", helperIdx...))
		scriptFile := fpInfo.GetSegmentHelperFilePath(contentID, filepath.FormatSuffix("script", helperIdx...))
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID, helperIdx...)
		backupFile := fpInfo.GetTableBackupFilePath(contentID, 0, GetPipeThroughProgram().Extension, true)
		helperCmdStr := fmt.Sprintf(`gpbackup_helper %s --toc-file %s --oid-file %s --pipe-file %s --data-file "%s" --content %d%s%s%s%s%s%s --copy-queue-size %d --verbosity %d`,
			operation, tocFile, oidFile, pipeFile, backupFile, contentID, pluginStr, compressStr, onErrorContinueStr, filterStr, singleDataFileStr, resizeStr, copyQueue, verbosity)
		// we run these commands in sequence to ensure that any failure is critical; the last command ensures the agent process was successfully started
		return fmt.Sprintf(`cat << HEREDOC > %[1]s && chmod +x %[1]s && ( nohup %[1]s &> /dev/null &)
#!/bin/bash
source %[2]s/greenplum_path.sh
%[2]s/bin/%s

HEREDOC

`, scriptFile, gphomePath, helperCmdStr)
	})
	c.CheckClusterError(remoteOutput, "Error starting gpbackup_helper agent", func(contentID int) string {
		return "Error starting gpbackup_helper agent"
	})
}

func findCommandStr(c *cluster.Cluster, fpInfo filepath.FilePathInfo, contentID int) string {
	var cmdString string
	if runtime.GOOS == "linux" {
		cmdString = fmt.Sprintf(`find %s -regextype posix-extended -regex ".*gpbackup_%d_%s_%d_(oid|script|pipe|skip).*"`,
			c.GetDirForContent(contentID), contentID, fpInfo.Timestamp, fpInfo.PID)
	} else if runtime.GOOS == "darwin" {
		cmdString = fmt.Sprintf(`find -E %s -regex ".*gpbackup_%d_%s_%d_(oid|script|pipe|skip).*"`,
			c.GetDirForContent(contentID), contentID, fpInfo.Timestamp, fpInfo.PID)
	}
	return cmdString
}

func GetHelperFileCount(c *cluster.Cluster, fpInfo filepath.FilePathInfo) int {
	totalFiles := 0
	remoteOutput := c.GenerateAndExecuteCommand("Checking for leftover gpbackup_helper files on segments", cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("%s %s", findCommandStr(c, fpInfo, contentID), "| wc -l")
	})

	if remoteOutput.NumErrors > 0 {
		gplog.Error("Unable to check for leftover gpbackup_helper files on segments")
	} else {
		for _, cmd := range remoteOutput.Commands {
			numFiles, _ := strconv.Atoi(strings.TrimSpace(cmd.Stdout))
			if numFiles > 0 {
				totalFiles = totalFiles + numFiles
			}
		}
	}
	return totalFiles
}

// Removes all gpbackup_helper files and pipes from the segment data directories.
// It's expected that gpbackup_helper cleans up the files on exit, but that is not guaranteed,
// so this function is used to clean up any leftover files.
func RemoveHelperFiles(c *cluster.Cluster, fpInfo filepath.FilePathInfo) {
	remoteOutput := c.GenerateAndExecuteCommand("Removing gpbackup_helper files from segment data directories", cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("%s %s", findCommandStr(c, fpInfo, contentID), `-exec rm -f {} \;`)
	})

	errMsg := fmt.Sprintf("Unable to remove gpbackup_helper file(s). See %s for a complete list of segments with errors and remove manually.",
		gplog.GetLogFilePath())
	c.CheckClusterError(remoteOutput, errMsg, func(contentID int) string {
		return fmt.Sprintf("Unable to remove gpbackup_helper file(s)\n\t%s", findCommandStr(c, fpInfo, contentID))
	}, true)
}

func CleanUpHelperFilesOnAllHosts(c *cluster.Cluster, fpInfo filepath.FilePathInfo, timeout time.Duration) {
	var fileCount int
	helperMutex.Lock()
	defer helperMutex.Unlock()
	tickerCleanup := time.NewTicker(1 * time.Second)

	for {
		fileCount = GetHelperFileCount(c, fpInfo)
		select {
		case <-tickerCleanup.C:
			if fileCount == 0 {
				return
			}
			RemoveHelperFiles(c, fpInfo)
		case <-time.After(timeout):
			gplog.Warn("Timeout of %ds reached while waiting for %d gpbackup_helper file(s) to be removed.", int(timeout.Seconds()), fileCount)
			return
		}
	}
}

// Checks for gpbackup_helper processes on segments.
// Returns a boolean indicating whether any PIDs were found and a map of hostnames to PIDs.
func CheckHelperPids(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string) (map[string][]int, bool) {
	var foundPid bool = false
	remoteOutput := c.GenerateAndExecuteCommand("Checking for leftover gpbackup_helper processes", cluster.ON_SEGMENTS, func(contentID int) string {
		tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
		procPattern := fmt.Sprintf("gpbackup_helper --%s-agent --toc-file %s*", operation, tocFile)
		pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)
		pipePattern := fmt.Sprintf("--pipe-file %s", pipeFile)
		return fmt.Sprintf(`ps ux | grep "%s" | grep "%s" | grep -v grep | awk '{print $2}'`, procPattern, pipePattern)
	})
	pidMap := make(map[string][]int)
	for _, cmd := range remoteOutput.Commands {
		pids := strings.Split(strings.TrimSpace(cmd.Stdout), "\n")
		host := c.GetHostForContent(cmd.Content)
		for _, pid := range pids {
			if pid != "" {
				foundPid = true
				gplog.Debug("Found gpbackup_helper process %s for segment %d on host %s", pid, cmd.Content, host)
				pidInt, _ := strconv.Atoi(pid)
				pidMap[host] = append(pidMap[host], pidInt)
			}
		}
	}
	return pidMap, foundPid
}

// Checks for gpbackup_helper processes on segments and sends a USR1 signal to them to terminate.
// If the processes are not terminated within the timeout, a warning is logged.
// Ideally, the termination requests would only be sent to the segment hosts reported by CheckHelperPids,
// but the current design of cluster.GenerateAndExecuteCommand does not allow for that, so we a
// command to check for pids and send the termination signal to all segments if CheckHelperPids reports
// that there are still processes running.
func CleanUpSegmentHelperProcesses(c *cluster.Cluster, fpInfo filepath.FilePathInfo, operation string, timeout time.Duration) {
	helperMutex.Lock()
	defer helperMutex.Unlock()
	tickerCleanup := time.NewTicker(1 * time.Second)

	for {
		helperPids, found := CheckHelperPids(c, fpInfo, operation)
		select {
		case <-tickerCleanup.C:
			if !found {
				return
			}
			c.GenerateAndExecuteCommand("Cleaning up gpbackup_helper processes", cluster.ON_SEGMENTS, func(contentID int) string {
				tocFile := fpInfo.GetSegmentTOCFilePath(contentID)
				procPattern := fmt.Sprintf("gpbackup_helper --%s-agent --toc-file %s", operation, tocFile)
				pipeFile := fpInfo.GetSegmentPipeFilePath(contentID)
				pipePattern := fmt.Sprintf("--pipe-file %s", pipeFile)
				return fmt.Sprintf("PIDS=`ps ux | grep \"%s\" | grep \"%s\" | grep -v grep | awk '{print $2}'`; if [[ ! -z \"$PIDS\" ]]; then kill -USR1 $PIDS; fi", procPattern, pipePattern)
			})
		case <-time.After(timeout):
			for host, pids := range helperPids {
				if len(pids) > 0 {
					gplog.Warn("Unable to terminate segment helper process(es) on host %s. ", host)
				}
			}
			gplog.Warn("See %s for a complete list of segments with errors and terminate manually.", gplog.GetLogFilePath())
			return
		}
	}
}

func CheckAgentErrorsOnSegments(c *cluster.Cluster, fpInfo filepath.FilePathInfo, helperIdx ...int) error {
	remoteOutput := c.GenerateAndExecuteCommand("Checking whether segment agents had errors", cluster.ON_SEGMENTS, func(contentID int) string {
		errorFile := GetErrorFilename(fpInfo.GetSegmentPipeFilePath(contentID, helperIdx...))
		if len(helperIdx) == 0 {
			errorFile += "*"
		}

		/*
		 * If an error file exists we want to indicate an error, as that means
		 * the agent errored out.  If no file exists, the agent was successful.
		 */
		return fmt.Sprintf("if ls %[1]s >/dev/null 2>/dev/null; then echo 'error'; fi; rm -f %[1]s", errorFile)
	})

	agent := filepath.FormatSuffix("agent", helperIdx...)
	numErrors := 0
	for contentID, cmd := range remoteOutput.Commands {
		if strings.TrimSpace(cmd.Stdout) == "error" {
			gplog.Verbose("Error occurred with helper %s on segment %d on host %s.", agent, contentID, c.GetHostForContent(contentID))
			numErrors++
		}
	}
	if numErrors > 0 {
		helperLogName := fpInfo.GetHelperLogPath()
		return errors.Wrapf(AgentErr, "Encountered errors with %d helper agent(s).  See %s for a complete list of segments with errors, and see %s on the corresponding hosts for detailed error messages.",
			numErrors, gplog.GetLogFilePath(), helperLogName)
	}
	return nil
}

func CreateSkipFileOnSegments(oid uint32, tableName string, c *cluster.Cluster, fpInfo filepath.FilePathInfo, helperIdx ...int) {
	createSkipFileLogMsg := fmt.Sprintf("Creating skip file on segments for restore entry %d (%s)", oid, tableName)
	remoteOutput := c.GenerateAndExecuteCommand(createSkipFileLogMsg, cluster.ON_SEGMENTS, func(contentID int) string {
		return fmt.Sprintf("touch %s_%d", GetSkipFilename(fpInfo.GetSegmentPipeFilePath(contentID, helperIdx...)), oid)
	})
	c.CheckClusterError(remoteOutput, "Error while creating skip file on segments", func(contentID int) string {
		return fmt.Sprintf("Could not create skip file %s_%d on segments", GetSkipFilename(fpInfo.GetSegmentPipeFilePath(contentID, helperIdx...)), oid)
	})
}

func GetErrorFilename(pipeFile string) string {
	return regexp.MustCompile(`_pipe(\d*)$`).ReplaceAllString(pipeFile, "_error$1")
}

func GetSkipFilename(pipeFile string) string {
	return regexp.MustCompile(`_pipe(\d*)$`).ReplaceAllString(pipeFile, "_skip$1")
}
