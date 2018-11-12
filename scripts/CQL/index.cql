CREATE INDEX IF NOT EXISTS indexSujet
ON testcluster.records (  sujet  );

CREATE INDEX IF NOT EXISTS indexPredicat
ON testcluster.records (  predicat );

CREATE INDEX IF NOT EXISTS indexObjet
ON testcluster.records (  objet );

DESC records;

DROP INDEX IF EXISTS indexSujet;
DROP INDEX IF EXISTS indexPredicat;
DROP INDEX IF EXISTS indexObjet;

DESC records;
