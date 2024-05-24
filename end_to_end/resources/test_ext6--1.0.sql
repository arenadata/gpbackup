/* src/test/modules/test_extensions/test_ext5--1.0.sql */
-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ext6" to load this file. \quit

CREATE TABLE t_part (id int, a int, b int , c text)
    DISTRIBUTED BY (id)
    PARTITION BY RANGE (a)
        SUBPARTITION BY LIST (c)
            SUBPARTITION TEMPLATE
            ( SUBPARTITION a_part VALUES ('a'),
            SUBPARTITION b_part VALUES ('b'))
        (START (0) INCLUSIVE
        END (3) EXCLUSIVE
        EVERY (1));

CREATE TABLE d_part (a int, b int, c int) DISTRIBUTED BY (a)
    PARTITION BY RANGE (b) (START (0) END (3) EVERY (1), default partition extra);
INSERT INTO d_part SELECT a, 10, a FROM generate_series(1, 100)a;

CREATE TABLE parent(a int, b int) DISTRIBUTED BY (a);
