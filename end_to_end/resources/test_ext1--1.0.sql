/* src/test/modules/test_extensions/test_ext1--1.0.sql */
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ext1" to load this file. \quit

CREATE TABLE test1 (i int) DISTRIBUTED BY (i);
CREATE TABLE test2 (i int, b bool) DISTRIBUTED BY (i);
SELECT pg_catalog.pg_extension_config_dump('test1', '');
SELECT pg_catalog.pg_extension_config_dump('test2', 'WHERE b');
