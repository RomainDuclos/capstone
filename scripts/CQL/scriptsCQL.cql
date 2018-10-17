-- cqlsh:testdata> SELECT sujet, predicat, objet, token(sujet), token(predicat), token(objet) from records limit 5;
--
--  sujet                 | predicat              | objet                   | system.token(sujet)  | system.token(predicat) | system.token(objet)
-- -----------------------+-----------------------+-------------------------+----------------------+------------------------+----------------------
--  http://example.org/s1 | http://example.org/p1 | http://example.org/o001 | -5250740383648889070 |    5723116222511537749 | -5759393648905835904
--  http://example.org/s1 | http://example.org/p1 | http://example.org/o002 | -5250740383648889070 |    5723116222511537749 | -5673800717780727381
--  http://example.org/s1 | http://example.org/p1 | http://example.org/o003 | -5250740383648889070 |    5723116222511537749 |  8277343976106184209
--  http://example.org/s1 | http://example.org/p1 | http://example.org/o004 | -5250740383648889070 |    5723116222511537749 | -5592515427346688361
--  http://example.org/s1 | http://example.org/p1 | http://example.org/o005 | -5250740383648889070 |    5723116222511537749 | -5329593732853982511
--
--
-- SELECT sujet,
--
-- cqlsh:testdata>
-- SELECT sujet, predicat, objet
-- from records
-- where token(sujet) = token('-5250740383648889070') and token(predicat) = token('5723116222511537749') and token(objet) = token('-5759393648905835904');
--
-- SELECT sujet, predicat, objet
-- from records
-- where token(sujet, predicat, objet) > token('-5250740383648889070', '5723116222511537749', '-5759393648905835904');

SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records limit 5;

cqlsh:jekasstout> SELECT sujet, predicat, objet, token(sujet, predicat, objet) from records limit 5;

 sujet                 | predicat              | objet                   | system.token(sujet, predicat, objet)
-----------------------+-----------------------+-------------------------+--------------------------------------
 http://example.org/s1 | http://example.org/p1 | http://example.org/o010 |                 -9215648761100528181
 http://example.org/s2 | http://example.org/p1 | http://example.org/o007 |                 -9039221394025946522
 http://example.org/s3 | http://example.org/p2 | http://example.org/o001 |                 -8815126328332944043
 http://example.org/s1 | http://example.org/p1 | http://example.org/o052 |                 -8739132598057725708
 http://example.org/s1 | http://example.org/p1 | http://example.org/o004 |                 -8421141122291269125

SELECT * from records where token(sujet, predicat, objet) > -9215648761100528181;
