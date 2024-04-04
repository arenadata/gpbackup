-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_ext6" to load this file. \quit

CREATE TABLE public.t_part (id int, a int, b int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (a)
( START (1) INCLUSIVE
   END (3) EXCLUSIVE
   EVERY (1) );

CREATE TABLE public.t_base (a int, b int) DISTRIBUTED BY (a);
