/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

CREATE TABLE Data_ (
       f1 STRING,
       f2 DATE,
       f3 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
TBLPROPERTIES('skip.header.line.count'='0');

LOAD DATA LOCAL INPATH 'data.tsv' OVERWRITE INTO TABLE Data_;

CREATE TABLE Conte AS
SELECT
    f1, COUNT(1) AS Cant
FROM
    Data_
GROUP BY
    f1
ORDER BY
    f1;

INSERT OVERWRITE LOCAL DIRECTORY './output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM Conte;
