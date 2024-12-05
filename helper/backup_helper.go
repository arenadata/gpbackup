package helper

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/greenplum-db/gpbackup/toc"
	"github.com/greenplum-db/gpbackup/utils"
	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

/*
 * Backup specific functions
 */

func doBackupAgent() error {
	var lastRead uint64
	var (
		pipeWriter BackupPipeWriterCloser
		writeCmd   *exec.Cmd
	)
	tocfile := &toc.SegmentTOC{}
	tocfile.DataEntries = make(map[uint]toc.SegmentDataEntry)

	oidList, err := getOidListFromFile(*oidFile)
	if err != nil {
		// error logging handled in getOidListFromFile
		return err
	}

	var currentPipe string
	var errBuf bytes.Buffer
	var readHandle *os.File
	var reader *bufio.Reader
	/*
	 * It is important that we create the reader before creating the writer
	 * so that we establish a connection to the first pipe (created by gpbackup)
	 * and properly clean it up if an error occurs while creating the writer.
	 */
	for i, oid := range oidList {
		currentPipe = fmt.Sprintf("%s_%d", *pipeFile, oidList[i])
		if wasTerminated.Load() {
			logError("Terminated due to user request")
			return errors.New("Terminated due to user request")
		}

		logInfo(fmt.Sprintf("Oid %d: Opening pipe %s", oid, currentPipe))
		for {
			reader, readHandle, err = getBackupPipeReader(currentPipe)
			if err != nil {
				if errors.Is(err, unix.ENXIO) || errors.Is(err, unix.ENOENT) {
					// keep trying to open the pipe
					time.Sleep(50 * time.Millisecond)
				} else {
					logError(fmt.Sprintf("Oid %d: Error encountered getting backup pipe reader: %v", oid, err))
					return err
				}
			} else {
				// A reader has connected to the pipe and we have successfully opened
				// the writer for the pipe. To avoid having to write complex buffer
				// logic for when os.write() returns EAGAIN due to full buffer, set
				// the file descriptor to block on IO.
				unix.SetNonblock(int(readHandle.Fd()), false)
				logVerbose(fmt.Sprintf("Oid %d, Reader connected to pipe %s", oid, path.Base(currentPipe)))
				break
			}
		}
		if i == 0 {
			pipeWriter, writeCmd, err = getBackupPipeWriter(&errBuf)
			if err != nil {
				logError(fmt.Sprintf("Oid %d: Error encountered getting backup pipe writer: %v", oid, err))
				return err
			}
		}

		logInfo(fmt.Sprintf("Oid %d: Backing up table with pipe %s", oid, currentPipe))
		numBytes, err := io.Copy(pipeWriter, reader)
		if err != nil {
			logError(fmt.Sprintf("Oid %d: Error encountered copying bytes from pipeWriter to reader: %v", oid, err))
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
		logInfo(fmt.Sprintf("Oid %d: Read %d bytes\n", oid, numBytes))

		lastProcessed := lastRead + uint64(numBytes)
		tocfile.AddSegmentDataEntry(uint(oid), lastRead, lastProcessed)
		lastRead = lastProcessed

		_ = readHandle.Close()
		logInfo(fmt.Sprintf("Oid %d: Deleting pipe: %s\n", oid, currentPipe))
		deletePipe(currentPipe)
	}

	_ = pipeWriter.Close()
	if *pluginConfigFile != "" {
		/*
		 * When using a plugin, the agent may take longer to finish than the
		 * main gpbackup process. We either write the TOC file if the agent finishes
		 * successfully or write an error file if it has an error after the COPYs have
		 * finished. We then wait on the gpbackup side until one of those files is
		 * written to verify the agent completed.
		 */
		logVerbose("Uploading remaining data to plugin destination")
		err := writeCmd.Wait()
		if err != nil {
			logError(fmt.Sprintf("Error encountered writing either TOC file or error file: %v", err))
			return errors.Wrap(err, strings.Trim(errBuf.String(), "\x00"))
		}
	}
	err = tocfile.WriteToFileAndMakeReadOnly(*tocFile)
	if err != nil {
		// error logging handled in util.go
		return err
	}
	logVerbose("Finished writing segment TOC")
	return nil
}

func getBackupPipeReader(currentPipe string) (*bufio.Reader, *os.File, error) {
	readHandle, err := os.OpenFile(currentPipe, os.O_RDONLY|unix.O_NONBLOCK, os.ModeNamedPipe)
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}

	reader := bufio.NewReader(struct{ io.ReadCloser }{readHandle})
	return reader, readHandle, nil
}

func getBackupPipeWriter(errBuf *bytes.Buffer) (pipe BackupPipeWriterCloser, writeCmd *exec.Cmd, err error) {
	var writeHandle io.WriteCloser
	if *pluginConfigFile != "" {
		writeCmd, writeHandle, err = startBackupPluginCommand(errBuf)
	} else {
		writeHandle, err = os.Create(*dataFile)
	}
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}

	if *compressionLevel == 0 {
		pipe = NewCommonBackupPipeWriterCloser(writeHandle)
		return
	}

	if *compressionType == "gzip" {
		pipe, err = NewGZipBackupPipeWriterCloser(writeHandle, *compressionLevel)
		return
	}
	if *compressionType == "zstd" {
		pipe, err = NewZSTDBackupPipeWriterCloser(writeHandle, *compressionLevel)
		return
	}

	writeHandle.Close()
	// error logging handled by calling functions
	return nil, nil, fmt.Errorf("unknown compression type '%s' (compression level %d)", *compressionType, *compressionLevel)
}

func startBackupPluginCommand(errBuf *bytes.Buffer) (*exec.Cmd, io.WriteCloser, error) {
	pluginConfig, err := utils.ReadPluginConfig(*pluginConfigFile)
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}
	cmdStr := fmt.Sprintf("%s backup_data %s %s", pluginConfig.ExecutablePath, pluginConfig.ConfigPath, *dataFile)
	writeCmd := exec.Command("bash", "-c", cmdStr)

	writeHandle, err := writeCmd.StdinPipe()
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}
	writeCmd.Stderr = errBuf
	err = writeCmd.Start()
	if err != nil {
		// error logging handled by calling functions
		return nil, nil, err
	}
	return writeCmd, writeHandle, nil
}
