DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"
WHERE "NumAgrupamento" = '14109' OR "NumAgrupamento" = '14114';

DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloCliente"
WHERE "NumAgrupamento" = '14109' OR "NumAgrupamento" = '14114';

DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda"
WHERE "NumAgrupamento" = '14109' OR "NumAgrupamento" = '14114';

DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.UltimasComprasHeader"
WHERE "NumAgrupamento" = '14109' OR "NumAgrupamento" = '14114';

DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.UltimasComprasItens"
WHERE "NumAgrupamento" = '14109' OR "NumAgrupamento" = '14114';