package helper

import (
	"bufio"
	"compress/gzip"
	"io"

	"github.com/greenplum-db/gpbackup/utils"
	"github.com/klauspost/compress/zstd"
)

type BackupPipeWriterCloser interface {
	io.Writer
	io.Closer
}

type CommonBackupPipeWriterCloser struct {
	writeHandle io.WriteCloser
	bufIoWriter *bufio.Writer
	finalWriter io.Writer
}

func (cPipe CommonBackupPipeWriterCloser) Write(p []byte) (n int, err error) {
	return cPipe.finalWriter.Write(p)
}

// Never returns error, suppressing them instead
func (cPipe CommonBackupPipeWriterCloser) Close() error {
	_ = cPipe.bufIoWriter.Flush()
	_ = cPipe.writeHandle.Close()
	return nil
}

func NewCommonBackupPipeWriterCloser(writeHandle io.WriteCloser, name string) (cPipe CommonBackupPipeWriterCloser, err error) {
	cPipe.writeHandle = writeHandle
	err, cPipe.bufIoWriter = utils.NewWriter(writeHandle, name)
	cPipe.finalWriter = cPipe.bufIoWriter
	return
}

type GZipBackupPipeWriterCloser struct {
	cPipe      CommonBackupPipeWriterCloser
	gzipWriter *gzip.Writer
}

func (gzPipe GZipBackupPipeWriterCloser) Write(p []byte) (n int, err error) {
	return gzPipe.gzipWriter.Write(p)
}

// Returns errors from underlying common writer only
func (gzPipe GZipBackupPipeWriterCloser) Close() error {
	_ = gzPipe.gzipWriter.Close()
	return gzPipe.cPipe.Close()
}

func NewGZipBackupPipeWriterCloser(writeHandle io.WriteCloser, compressLevel int, name string) (gzPipe GZipBackupPipeWriterCloser, err error) {
	gzPipe.cPipe, err = NewCommonBackupPipeWriterCloser(writeHandle, name)
	if err != nil {
		// error logging handled by calling functions
		return
	}
	gzPipe.gzipWriter, err = gzip.NewWriterLevel(gzPipe.cPipe.bufIoWriter, compressLevel)
	if err != nil {
		gzPipe.cPipe.Close()
	}
	return
}

type ZSTDBackupPipeWriterCloser struct {
	cPipe       CommonBackupPipeWriterCloser
	zstdEncoder *zstd.Encoder
}

func (zstdPipe ZSTDBackupPipeWriterCloser) Write(p []byte) (n int, err error) {
	return zstdPipe.zstdEncoder.Write(p)
}

// Returns errors from underlying common writer only
func (zstdPipe ZSTDBackupPipeWriterCloser) Close() error {
	_ = zstdPipe.zstdEncoder.Close()
	return zstdPipe.cPipe.Close()
}

func NewZSTDBackupPipeWriterCloser(writeHandle io.WriteCloser, compressLevel int, name string) (zstdPipe ZSTDBackupPipeWriterCloser, err error) {
	zstdPipe.cPipe, err = NewCommonBackupPipeWriterCloser(writeHandle, name)
	if err != nil {
		// error logging handled by calling functions
		return
	}
	zstdPipe.zstdEncoder, err = zstd.NewWriter(zstdPipe.cPipe.bufIoWriter, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(compressLevel)))
	if err != nil {
		zstdPipe.cPipe.Close()
	}
	return
}
