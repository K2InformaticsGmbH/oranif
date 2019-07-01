Rem
Rem travis.sql
Rem

SET FEEDBACK 1
SET NUMWIDTH 10
SET LINESIZE 80
SET TRIMSPOOL ON
SET TAB OFF
SET PAGESIZE 999
SET ECHO ON
SET CONCAT '.'
SET SHOWMODE OFF
SET VERIFY OFF

DEFINE logfile_dir = &1

host mkdir &&logfile_dir
CONNECT system/oracle@0.0.0.0:1521/xe
SET SHOWMODE OFF

DEFINE spool_file = &&logfile_dir.hr_main.log
SPOOL &spool_file

CREATE USER scott IDENTIFIED BY regit;

GRANT ALTER SYSTEM TO scott;
GRANT CREATE SESSION TO scott;
GRANT UNLIMITED TABLESPACE TO scott;
GRANT CREATE TABLE TO scott;
GRANT CREATE PROCEDURE TO scott;
