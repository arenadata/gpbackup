#!/bin/bash
set -e

backup_file() {
  echo "backup_file $1 $2" >> /tmp/plugin_out.txt
  cat $2 >> /dev/null
}

backup_data() {
  echo "backup_data $1 $2" >> /tmp/plugin_out.txt
  cat - >> /dev/null
}

# functions for API compatibility
setup_plugin_for_backup() {
  echo "setup_plugin_for_backup" >> /tmp/plugin_out.txt
}

setup_plugin_for_restore() {
  echo "restore not supported" 1>&2;
  exit 1
}

cleanup_plugin_for_backup() {
  echo "cleanup_plugin_for_backup: nothing to do" >> /tmp/plugin_out.txt
}

cleanup_plugin_for_restore() {
  echo "restore not supported" 1>&2;
  exit 1
}

restore_file() {
  echo "restore not supported" 1>&2;
  exit 1
}

restore_data() {
  echo "restore not supported" 1>&2;
  exit 1
}

delete_backup() {
  echo "delete_backup: nothing to do" >> /tmp/plugin_out.txt
}

list_directory() {
  echo "list_directory" >> /tmp/plugin_out.txt
  echo '/dev/null'
}

delete_directory() {
  echo "delete_directory: nothing to do" >> /tmp/plugin_out.txt
}

plugin_api_version() {
  echo "0.5.0"
  echo "0.5.0" >> /tmp/plugin_out.txt
}

--version() {
  echo "null_plugin version 1.0.0"
  echo "null_plugin version 1.0.0" >> /tmp/plugin_out.txt
}

"$@"
