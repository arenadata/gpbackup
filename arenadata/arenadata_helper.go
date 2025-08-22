package arenadata

import (
	"github.com/GreengageDB/gp-common-go-libs/dbconn"
	"github.com/greenplum-db/gpbackup/history"
	"github.com/greenplum-db/gpbackup/toc"
	"regexp"
	"strconv"
	"strings"

	"github.com/GreengageDB/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

var (
	adPattern = regexp.MustCompile(`_arenadata(\d+)`)
)

func EnsureAdVersionCompatibility(backupVersion string, restoreVersion string) {
	adBackup := getArenadataVersion(backupVersion)
	adRestore := getArenadataVersion(restoreVersion)

	if adRestore < adBackup {
		gplog.Fatal(errors.Errorf("gprestore arenadata%d cannot restore a backup taken with gpbackup arenadata%d; please use gprestore arenadata%d or later.",
			adRestore, adBackup, adBackup), "")
	}
}

// fullVersion: gpbackup version + '_' + arenadata release + ('+' + gpbackup build)
// example: 1.20.4_arenadata2+dev.1.g768b7e0 -> 1.20.4+dev.1.g768b7e0
func GetOriginalVersion(fullVersion string) string {
	return adPattern.ReplaceAllString(fullVersion, "")
}

func PatchStatisticsStatements(backupConfig *history.BackupConfig, connectionPool *dbconn.DBConn, statements []toc.StatementWithType) []toc.StatementWithType {
	// Backups created in versions 1.30.5_arenadata16 to 1.30.5_arenadata19 have ineffective sql for
	// deleting statistics, which can affect restore performance. as a workaround for these
	// versions, we enable nested loop.
	if connectionPool.Version.Is("6") &&
		strings.Contains(backupConfig.BackupVersion, "1.30.5_arenadata") {
		arenadataVersion := getArenadataVersion(backupConfig.BackupVersion)

		if arenadataVersion >= 16 && arenadataVersion <= 19 {
			statements = append(statements, toc.StatementWithType{})
			copy(statements[1:], statements[:])
			statements[0] = toc.StatementWithType{
				Statement: "SET enable_nestloop = ON;",
			}
		}
	}
	return statements
}

func getArenadataVersion(fullVersion string) uint {
	match := adPattern.FindStringSubmatch(fullVersion)
	if len(match) != 2 {
		gplog.Fatal(errors.Errorf("Invalid arenadata version format for gpbackup: %s", fullVersion), "")
	}
	result, err := strconv.ParseUint(match[1], 10, 32)
	gplog.FatalOnError(err)
	return uint(result)
}
