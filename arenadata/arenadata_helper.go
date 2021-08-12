package arenadata

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

func EnsureAdVersionCompatibility(backupVersion string, restoreVersion string) {
	regex := *regexp.MustCompile(`_arenadata(\d+)`)
	adBackup := regex.FindAllStringSubmatch(backupVersion, -1)[0][1]
	adRestore := regex.FindAllStringSubmatch(restoreVersion, -1)[0][1]
	if adRestore < adBackup {
		gplog.Fatal(errors.Errorf("gprestore arenadata%s cannot restore a backup taken with gpbackup arenadata%s; please use gprestore arenadata%s or later.",
			adRestore, adBackup, adBackup), "")
	}
}

// fullVersion: gpbackup version + '_' + arenadata release + ('+' + gpbackup build)
// example: 1.20.4_arenadata2+dev.1.g768b7e0 -> 1.20.4+dev.1.g768b7e0
func GetOriginalVersion(fullVersion string) (pureVersion string, err error) {
	base := strings.SplitN(fullVersion, "_", 2)
	pureVersion = base[0]
	adRelease := base[1]

	if idx := strings.LastIndex(base[1], "+"); idx != -1 {
		// dev version
		build := base[1][idx:]
		adRelease = base[1][:idx]
		pureVersion += build
	}

	patern := regexp.MustCompile(`^arenadata\d+$`)
	if adIndex := patern.FindStringIndex(adRelease); adIndex == nil {
		return pureVersion, fmt.Errorf("Invalid format of arenadata build string %s\n", adRelease)
	}
	return
}
