-- SELECT *
-- FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"
-- WHERE "NumAgrupamento" = '25567'
-- 	OR "NumAgrupamento" = '25568'
-- 	OR "NumAgrupamento" = '25569'
-- 	OR "NumAgrupamento" = '25570'
-- 	OR "NumAgrupamento" = '25571';

-- UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"
-- SET "Status" = 'Pausada', "Excluido" = ''
-- WHERE "EmailVendedor" = 'ext.felipe.dias@vcimentos.com' AND "SLA_OTIF" > '2020-06-20T00:00:00';

SELECT *
FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda"
WHERE "OrdemVenda" = 'ext.felipe.dias@vcimentos.com';

-- ("Key", "Value") VALUES
-- ('FiveNineCampanha', 'CRC_FullCharge_Homolog');

-- DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.EnvFiveNine"
-- WHERE "OrdemVenda" = '1111'

SELECT SUM(o."ToneladasOV") FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" o WHERE '4144' = o."Centro" AND o."DataOrdemVenda" BETWEEN '2020-06-29T00:00:00' AND '2020-06-29T23:59:59'

-- SELECT *
-- FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"
-- WHERE "AtendimentoFim" BETWEEN '2020-06-16T00:00:00' AND '2020-06-16T23:59:59';
