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
