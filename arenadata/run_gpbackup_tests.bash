#!/bin/bash -l

set -eox pipefail

# 7x has additional tests with de_DE locale. Install the missing.
# 6x has no such package and following commands are excessive, but it's not an error.
yum install -y glibc-locale-source || true
localedef -i de_DE -f UTF-8 de_DE

source gpdb_src/concourse/scripts/common.bash
install_and_configure_gpdb
make -C gpdb_src/src/test/regress/
# dummy_seclabel has different installation path for 6x and 7x. Try both.
if ! make -C gpdb_src/contrib/dummy_seclabel/ install
then
	make -C gpdb_src/src/test/modules/dummy_seclabel/ install
fi
gpdb_src/concourse/scripts/setup_gpadmin_user.bash
make_cluster

wget https://golang.org/dl/go1.20.5.linux-amd64.tar.gz -O - | tar -C /opt -xz;

su - gpadmin -c "
source /usr/local/greenplum-db-devel/greenplum_path.sh;
source ~/gpdb_src/gpAux/gpdemo/gpdemo-env.sh;
gpconfig -c shared_preload_libraries -v dummy_seclabel;
gpstop -ar;
PATH=$PATH:/opt/go/bin:~/go/bin GOPATH=~/go make depend build install integration end_to_end -C /home/gpadmin/go/src/github.com/greenplum-db/gpbackup"
