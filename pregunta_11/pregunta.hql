/*

Pregunta
===========================================================================

Escriba una consulta que retorne la primera columna, la cantidad de 
elementos en la columna 2 y la cantidad de elementos en la columna 3
Apache Hive se ejecutará en modo local (sin HDFS).
Escriba el resultado a la carpeta `output` de directorio de trabajo.

    >>> Escriba su respuesta a partir de este punto <<<
*/
CREATE TABLE t0 (
    c1 STRING,
    c2 ARRAY<CHAR(1)>,
    c3 MAP<STRING, INT> )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY '#' LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH 'data.tsv' INTO TABLE t0;

CREATE TABLE ANS_1 AS
SELECT
    c1, SIZE(c2), SIZE(c3)
FROM t0;

INSERT OVERWRITE LOCAL DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM ANS_1;