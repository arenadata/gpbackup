package arenadata

import (
	"regexp"
	"strconv"

	"github.com/GreengageDB/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

var (
	adPattern = regexp.MustCompile(`_arenadata(\d+)`)
)

func EnsureAdVersionCompatibility(backupVersion string, restoreVersion string) {
	adBackup := GetArenadataVersion(backupVersion)
	adRestore := GetArenadataVersion(restoreVersion)

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

func GetArenadataVersion(fullVersion string) uint {
	match := adPattern.FindStringSubmatch(fullVersion)
	if len(match) != 2 {
		gplog.Fatal(errors.Errorf("Invalid arenadata version format for gpbackup: %s", fullVersion), "")
	}
	result, err := strconv.ParseUint(match[1], 10, 32)
	gplog.FatalOnError(err)
	return uint(result)
}
