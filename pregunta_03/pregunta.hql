/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
CREATE TABLE Data_(
    f1 STRING,
    f2 DATE,
    f3 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES ('skip.header.line.count'='0');
LOAD DATA LOCAL INPATH 'data.tsv' OVERWRITE INTO TABLE Data_;

CREATE TABLE dist_ AS
SELECT
    DISTINCT f3
FROM
    Data_;

INSERT OVERWRITE LOCAL DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM dist_;

