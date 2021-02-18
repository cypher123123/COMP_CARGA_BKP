let oConn = $.db.getConnection();

function getOTIFData(sTimeInicio, sTimeFim, propKey) {
    function getFromDB () {
        let query    = ' SELECT "Status", "SLA_OTIF", "AtendimentoFim", "EmailVendedor", "KPIValido", "InterfaceAtuante", "UnidadeCodigo" ';
        query       += ' FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
        query       += ' WHERE ("Status" = \'Complemento Total\' OR "Status" = \'Complemento Parcial\' OR "Status" = \'Não Complementado\') ';
        query       += ' AND "Excluido" = \'\' AND "AtendimentoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\' ';
        let stmt = oConn.prepareStatement(query);
        
        let resultSet = stmt.executeQuery();
        
        let results = [];
        
        while (resultSet.next()) {
            const data = {
                Status: resultSet.getNString(1),
                OTIF: resultSet.getTimestamp(2),
                AtendimentoFim: resultSet.getTimestamp(3),
                EmailVendedor: resultSet.getNString(4),
                KPIValido: resultSet.getNString(5)
            };
            
            if (data.AtendimentoFim < data.OTIF) {
                results.push(data);
            } else if (data.EmailVendedor) {
                if (data.KPIValido) {
                    results.push(data);
                }
            } else {
                results.push(data); 
            }
        }
        
        return results;
    }
    
    function calculateAndGroup(data) {
        const grouped = data.reduce(function (acc, item) {
            if (!acc[item[propKey]]) {
                acc[item[propKey]] = [];
            }
            
            acc[item[propKey]].push(item);
            
            return acc;
        }, {});
        
        const keys = Object.keys(grouped);
        
        return keys.reduce(function (accKeys, key) {
            const items = grouped[key];
            const dentroOTIF = items.reduce(function (accItems, item) {
                return item.AtendimentoFim > item.OTIF ? (accItems) : (accItems + 1);
            }, 0);
            
            accKeys[key] = {
                DENT_OTIF: dentroOTIF,
                FORA_OTIF: items.length - dentroOTIF,
                TOTAL: items.length
            };
            
            return accKeys;
        }, {});
    }
    
    const dbData = getFromDB();
    
    return calculateAndGroup(dbData);
}

function selectSummary(sTimeInicio, sTimeFim) {
    let protocoloKPIFilter = 'a."KPIValido"=\'X\'',
	 protocoloDateFilter = 'a."AtendimentoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'',
	 protocoloFilter = protocoloDateFilter +  ' AND ' + protocoloKPIFilter;
    let dateFilterNT = 'a."DataHoraCarga" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
    let tempoAtendimento = '(SECONDS_BETWEEN(a."AtendimentoInicio", a."AtendimentoFim") - a."PausaEmSegundos") / 60';
    let SLAPadraoMinutos = '(SELECT "SLAPadraoMinutos" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros")';
    
    let sQuery = 'SELECT COUNT(*) AS QTDE_AGRU,';
    sQuery += ' (select COUNT(a."NumAgrupamento") FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Total\' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS QTDE_AGCT,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Parcial\' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS QTDE_AGCP,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Não Complementado\' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS QTDE_AGIC,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Excluido" = \'X\' AND ' + dateFilterNT + ') AS QTDE_AGNT,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Total\' AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS DENT_ATEN_AGCT,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Parcial\' AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS DENT_ATEN_AGCP,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Não Complementado\' AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS DENT_ATEN_AGIC,';
    
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Total\' AND ' + tempoAtendimento + ' > ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS DENT_ATEN_AGCT,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Parcial\' AND ' + tempoAtendimento + ' > ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS DENT_ATEN_AGCP,';
    sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Não Complementado\' AND ' + tempoAtendimento + ' > ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS DENT_ATEN_AGIC';
    
    sQuery += ' FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" != \'Em Atendimento\' AND ' + dateFilterNT + ' OR ' + protocoloDateFilter;
//   let sQuery = 'SELECT COUNT(*) AS QTDE_AGRU,';
//     sQuery += ' (select COUNT(a."NumAgrupamento") FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Total\' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS QTDE_AGCT,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Parcial\' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS QTDE_AGCP,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Não Complementado\' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS QTDE_AGIC,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Excluido" = \'X\' AND ' + dateFilterNT + ') AS QTDE_AGNT,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Total\' and (a."AtendimentoFim" < a."SLA_OTIF" OR NOW() < a."SLA_OTIF") AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_OTIF_AGCT,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Parcial\' and (a."AtendimentoFim" < a."SLA_OTIF" OR NOW() < a."SLA_OTIF") AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_OTIF_AGCP,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Não Complementado\' and (a."AtendimentoFim" < a."SLA_OTIF" OR NOW() < a."SLA_OTIF") AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_OTIF_AGIC,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Total\' AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_ATEN_AGCT,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Complemento Parcial\' AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_ATEN_AGCP,';
//     sQuery += ' (select COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" = \'Não Complementado\' AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_ATEN_AGIC';
//     sQuery += ' FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a where a."Status" != \'Em Atendimento\' AND ' + dateFilterNT + ' OR ' + protocoloDateFilter;
    let oStmt = oConn.prepareStatement(sQuery);
    
    let sResultSet = oStmt.executeQuery();
    const OTIFData = getOTIFData(sTimeInicio, sTimeFim, 'Status');
    
    // return OTIFData;
    
    var QTDE_AGRU = 0;
    var QTDE_AGCT = 0;
    var QTDE_AGCP = 0;
    var QTDE_AGIC = 0;
    var QTDE_AGNT = 0;
    var DENT_ATEN_AGCT = 0;
    var DENT_ATEN_AGCP = 0;
    var DENT_ATEN_AGIC = 0;
    var FORA_ATEN_AGCT = 0;
    var FORA_ATEN_AGCP = 0;
    var FORA_ATEN_AGIC = 0;
    
    while(sResultSet.next()) {
        QTDE_AGCT = parseInt(sResultSet.getNString(2), 10);
        QTDE_AGCP = parseInt(sResultSet.getNString(3), 10);
        QTDE_AGIC = parseInt(sResultSet.getNString(4), 10);
        QTDE_AGNT = parseInt(sResultSet.getNString(5), 10);
        DENT_ATEN_AGCT = parseInt(sResultSet.getNString(6), 10);
        DENT_ATEN_AGCP = parseInt(sResultSet.getNString(7), 10);
        DENT_ATEN_AGIC = parseInt(sResultSet.getNString(8), 10);
        
        
        FORA_ATEN_AGCT = parseInt(sResultSet.getNString(9), 10);
        FORA_ATEN_AGCP = parseInt(sResultSet.getNString(10), 10);
        FORA_ATEN_AGIC = parseInt(sResultSet.getNString(11), 10);
        // DENT_OTIF_AGCT = parseInt(sResultSet.getNString(6), 10);
        // DENT_OTIF_AGCP = parseInt(sResultSet.getNString(7), 10);
        // DENT_OTIF_AGIC = parseInt(sResultSet.getNString(8), 10);
        // DENT_ATEN_AGCT = parseInt(sResultSet.getNString(9), 10);
        // DENT_ATEN_AGCP = parseInt(sResultSet.getNString(10), 10);
        // DENT_ATEN_AGIC = parseInt(sResultSet.getNString(11), 10);
    }
    
    QTDE_AGRU = QTDE_AGCT + QTDE_AGCP + QTDE_AGIC + QTDE_AGNT;
    var PERC_AGCT = parseFloat((QTDE_AGCT/QTDE_AGRU*100).toFixed(2));
    var PERC_AGCP = parseFloat((QTDE_AGCP/QTDE_AGRU*100).toFixed(2));
    var PERC_AGIC = parseFloat((QTDE_AGIC/QTDE_AGRU*100).toFixed(2));
    var PERC_AGNT = parseFloat((QTDE_AGNT/QTDE_AGRU*100).toFixed(2));
    // var FORA_ATEN_AGCT = QTDE_AGCT-DENT_ATEN_AGCT;
    // var FORA_ATEN_AGCP = QTDE_AGCP-DENT_ATEN_AGCP;
    // var FORA_ATEN_AGIC = QTDE_AGIC-DENT_ATEN_AGIC;
    
    var DENT_OTIF_AGCT = OTIFData['Complemento Total'] ? OTIFData['Complemento Total'].DENT_OTIF : 0;
    var DENT_OTIF_AGCP = OTIFData['Complemento Parcial'] ? OTIFData['Complemento Parcial'].DENT_OTIF : 0;
    var DENT_OTIF_AGIC = OTIFData['Não Complementado'] ? OTIFData['Não Complementado'].DENT_OTIF : 0;
    
    var FORA_OTIF_AGCT = OTIFData['Complemento Total'] ? OTIFData['Complemento Total'].FORA_OTIF : 0;
    var FORA_OTIF_AGCP = OTIFData['Complemento Parcial'] ? OTIFData['Complemento Parcial'].FORA_OTIF : 0;
    var FORA_OTIF_AGIC = OTIFData['Não Complementado'] ? OTIFData['Não Complementado'].FORA_OTIF : 0;
    
    return {
        QTDE_AGRU: QTDE_AGRU,
        QTDE_AGCT: QTDE_AGCT,
        QTDE_AGCP: QTDE_AGCP,
        QTDE_AGIC: QTDE_AGIC,
        QTDE_AGNT: QTDE_AGNT,
        DENT_OTIF_AGCT: DENT_OTIF_AGCT,
        DENT_OTIF_AGCP: DENT_OTIF_AGCP,
        DENT_OTIF_AGIC: DENT_OTIF_AGIC,
        DENT_ATEN_AGCT: DENT_ATEN_AGCT,
        DENT_ATEN_AGCP: DENT_ATEN_AGCP,
        DENT_ATEN_AGIC: DENT_ATEN_AGIC,
        PERC_AGCT: isNaN(PERC_AGCT) ? 0 : PERC_AGCT,
        PERC_AGCP: isNaN(PERC_AGCP) ? 0 : PERC_AGCP,
        PERC_AGIC: isNaN(PERC_AGIC) ? 0 : PERC_AGIC,
        PERC_AGNT: isNaN(PERC_AGNT) ? 0 : PERC_AGNT,
        FORA_OTIF_AGCT: FORA_OTIF_AGCT,
        FORA_OTIF_AGCP: FORA_OTIF_AGCP,
        FORA_OTIF_AGIC: FORA_OTIF_AGIC,
        FORA_ATEN_AGCT: FORA_ATEN_AGCT,
        FORA_ATEN_AGCP: FORA_ATEN_AGCP,
        FORA_ATEN_AGIC: FORA_ATEN_AGIC
    };
}

function mainFunction() {
    let sDataIni = $.request.parameters.get("DataIni");
    let sDataFim = $.request.parameters.get("DataFim");
    if(sDataIni && sDataFim) {
        let sTimeInicio = sDataIni + "T00:00:00";
        let sTimeFim = sDataFim + "T23:59:59";
        
        let response = selectSummary(sTimeInicio, sTimeFim);
        
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(response));
    }
    else {
        $.response.status = $.net.http.OK;
        $.response.contentType = "text/html";
        $.response.setBody("service working");
    }
}

mainFunction();
oConn.close();