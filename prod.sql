--------------------------------------------------------------------------------
/**
Autor: Quetzaltlali
Descripción: Creacion tablas
**/

-- Se crea la tabla 'productos' en el esquema 'data', con las columnas:
-- idep: un identificador de dependencia (opcional)
-- idproducto: identificador único para cada producto
-- prod: nombre del producto
-- idsubprod: identificador del subproducto
-- subprod: nombre del subproducto

--------------------------------------------------------------------------------
CREATE TABLE data.productos (
  idep int8 NULL,               -- Permite valores nulos para el identificador de dependencia
  idproducto int8 NOT NULL,     -- No permite valores nulos para el identificador del producto
  prod varchar(50) NULL,        -- Permite valores nulos para el nombre del producto
  idsubprod int8 NOT NULL,      -- No permite valores nulos para el identificador del subproducto
  subprod varchar(50) NULL      -- Permite valores nulos para el nombre del subproducto
);
--------------------------------------------------------------------------------
-- Se consulta el nombre de las tablas en el esquema 'data'
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'data';
--------------------------------------------------------------------------------
-- Se insertan datos en la tabla 'productos', asignando valores a las columnas correspondientes
-- En este caso, se usan NULL para el campo 'idep' (que no es obligatorio)

INSERT INTO data.productos (idDep, idproducto, prod, idsubprod, subprod)
VALUES
(NULL, 101, 'Nomina', 1001, 'Tradicional'),
(NULL, 102, 'Nomina', 1002, 'Refinanciamiento'),
(NULL, 201, 'Domiciliacion', 2001, 'Tradicional'),
(NULL, 202, 'Domiciliacion', 2002, 'Refinanciamiento'),
(NULL, 101, 'Nomina', 1003, 'Remodela'),
(NULL, 102, 'Nomina', 1004, 'Liquidación a competidores');

------------------
  
-- Se crea la tabla 'convenios' en el esquema 'data', con las columnas:
-- NombreConvenio: nombre del convenio
-- IdDependencia: identificador de la dependencia
-- NombrePagaduria: nombre de la pagaduría
-- IdPagaduria: identificador de la pagaduría


CREATE TABLE data.convenios (
  NombreConvenio varchar(100) NOT NULL,   -- Nombre del convenio, no permite NULL
  IdDependencia int8 NOT NULL,            -- Identificador de dependencia, no permite NULL
  NombrePagaduria varchar(100) NOT NULL,  -- Nombre de la pagaduría, no permite NULL
  IdPagaduria int8 NOT NULL              -- Identificador de pagaduría, no permite NULL
);
--------------------------------------------------------------------------------
-- Se insertan datos en la tabla 'convenios' con valores específicos
INSERT INTO data.convenios (NombreConvenio, IdDependencia, NombrePagaduria, IdPagaduria)
VALUES
('Gobierno de Veracruz', 57, 'Gobierno de Veracruz', 52),
('SNTE 23 Puebla', 58, 'Federal', 53),
('Federalizados', 54, 'Federal', 54);

-- Se consulta el contenido de la tabla 'convenios'
SELECT * FROM data.convenios;

-- Se consulta el contenido de la tabla 'productos'
SELECT * FROM data.productos;

-- Se renombra la columna 'iddependencia' a 'iddep' en la tabla 'convenios'
ALTER TABLE data.convenios RENAME COLUMN iddependencia TO iddep;

-- Se consulta nuevamente la tabla 'productos' para verificar los cambios
SELECT * FROM data.productos;

-- Se agrega una nueva columna llamada 'IdPagaduria' en la tabla 'productos'
ALTER TABLE data.productos ADD COLUMN IdPagaduria int8;
--------------------------------------------------------------------------------
-- Se actualizan los valores de 'idproducto' en la tabla 'productos' basándose en el nombre del producto ('prod')
-- Si el producto es 'Nomina', se asigna el valor 101, y si es 'Domiciliacion', se asigna el valor 102
-- Si no es ninguno de estos, el valor de 'idproducto' se mantiene sin cambios

UPDATE data.productos
SET idproducto = 
  CASE 
WHEN prod = 'Nomina' THEN 101
WHEN prod = 'Domiciliacion' THEN 102
ELSE idproducto
END;
--------------------------------------------------------------------------------
-- Se actualizan los valores de 'idsubprod' en la tabla 'productos' basándose en el nombre del subproducto ('subprod')
-- Si el subproducto es 'Tradicional', se asigna el valor 01, si es 'Refinanciamiento' se asigna el valor 02,
-- si es 'Remodela' se asigna el valor 03, y si es 'Liquidación a competidores', se asigna el valor 04
-- Si no es ninguno de estos, el valor de 'idsubprod' se mantiene sin cambios


UPDATE data.productos
SET idsubprod = 
  CASE 
WHEN subprod = 'Tradicional' THEN 01
WHEN subprod = 'Refinanciamiento' THEN 02
WHEN subprod = 'Remodela' THEN 03
WHEN subprod = 'Liquidación a competidores' THEN 04
ELSE idproducto
END;
--------------------------------------------------------------------------------
  
