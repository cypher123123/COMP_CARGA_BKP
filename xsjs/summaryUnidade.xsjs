let oConn = $.db.getConnection();

function getOTIFData(sTimeInicio, sTimeFim, propKey) {
    function getFromDB () {
        let query    = ' SELECT "Status", "SLA_OTIF", "AtendimentoFim", "EmailVendedor", "KPIValido", "UnidadeCodigo" ';
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
                KPIValido: resultSet.getNString(5),
                Centro: resultSet.getNString(6)
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
    
    let tProtocolo = '"COMP_CARGA"."comp_carga.table::cds_table.Protocolo"',
	    tOrdemVenda = '"COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda"',
	    tCentro = '"COMP_CARGA"."comp_carga.table::cds_table.CentroRegiao"',
	    wUnidade = 'uni."CodCentro" = a."UnidadeCodigo"',
	   // cVolume = 'SUM(a."ToneladasRestantesInicial" - a."ToneladasRestantes")',
	    cTempo = 'AVG(SECONDS_BETWEEN(a."AtendimentoInicio",a."AtendimentoFim") - a."PausaEmSegundos")';
	 
	let protocoloKPIFilter = 'a."KPIValido"=\'X\'',
    	 protocoloDateFilter = 'a."AtendimentoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'',
    	 protocoloFilter = protocoloDateFilter +  ' AND ' + protocoloKPIFilter;
	let ovFilter = 'o."DataOrdemVenda" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
	let tempoAtendimento = '(SECONDS_BETWEEN(a."AtendimentoInicio", a."AtendimentoFim") - a."PausaEmSegundos") / 60';
    let SLAPadraoMinutos = '(SELECT "SLAPadraoMinutos" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros")';
    
    let sQuery =  'SELECT uni."CodCentro" as UNID_NOME,';
        sQuery += ' (SELECT SUM(a."ToneladasRestantesInicial") FROM '+tProtocolo+' a WHERE '+wUnidade+' AND a."SLA_OTIF" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\' AND a."Excluido" = \'\') as VOLU_AGRU,';
        sQuery += ' (SELECT SUM(a."ToneladasRestantes") FROM ' + tProtocolo + ' a WHERE ' + wUnidade + ' AND a."SLA_OTIF" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\' AND a."Excluido" = \'\') as VOLU_NECS,';
        // sQuery += ' (SELECT SUM(a."ToneladasRestantes") FROM '+tProtocolo+' a WHERE '+wUnidade+' AND ' + dateFilter + ') as VOLU_NECS,';
        sQuery += ' (SELECT SUM(o."ToneladasOV") FROM ' + tOrdemVenda + ' o WHERE uni."CodCentro" = o."Centro" AND ' + ovFilter + ') as VOLU_GRCM,';
        // sQuery += ' (SELECT (SELECT SUM(o."ToneladasOV") FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" o WHERE o."Centro" = uni."CodCentro" AND ' + ovFilter + ')/SUM(a."ToneladasRestantesInicial") From ' + tProtocolo + ' a where ' + dateFilter + ') AS PERC_CMNS,';
        sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE a."Status" = '+"'Complemento Total'"+' and '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') as QTDE_AGCT,'; 
        sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE a."Status" = '+"'Complemento Parcial'"+' and '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') as QTDE_AGCP,';
        sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE a."Status" = '+"'Não Complementado'"+' and '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') as QTDE_AGIC,';
        sQuery += ' (SELECT COUNT(*) FROM '+tOrdemVenda+' o INNER JOIN '+tProtocolo+' a on a."NumAgrupamento" = o."NumAgrupamento"  WHERE '+wUnidade+' AND ' + ovFilter + ' AND a."Excluido" = \'\') as QTDE_PDGR,';
        sQuery += ' (SELECT '+cTempo+' FROM '+tProtocolo+' a WHERE '+wUnidade+' AND ' + protocoloFilter + ' AND a."Excluido" = \'\')  as TEMP_SLAA,'; 
        sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."CodCentro" = a."UnidadeCodigo" AND a."AtendimentoFim" <= a."SLA_OTIF" AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_OTIF,';
        sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."CodCentro" = a."UnidadeCodigo" AND a."AtendimentoFim" > a."SLA_OTIF" AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS FORA_OTIF,';
        sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."CodCentro" = a."UnidadeCodigo" AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS DENT_ATEN,';
        sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."CodCentro" = a."UnidadeCodigo" AND ' + tempoAtendimento + ' > ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\' AND a."AtendimentoInicio" IS NOT NULL) AS FORA_ATEN,';
        sQuery += ' (SELECT DISTINCT a."UnidadeNome" FROM '+tProtocolo+' a WHERE uni."CodCentro" = a."UnidadeCodigo") AS UNID_NOME,';
        sQuery += ' (SELECT SUM(a."FreteMorto") FROM ' + tProtocolo + ' a WHERE uni."CodCentro" = a."UnidadeCodigo" AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS VALR_FTMR,'; 
        sQuery += ' uni."NomeRegiao" AS UNID_REGI,';
        sQuery += ' (SELECT SUM(o."Isencao") FROM ' + tOrdemVenda + ' o WHERE o."Centro" = uni."CodCentro" AND ' + ovFilter + ') AS VALR_ISEN';
        sQuery += ' FROM (SELECT c."CodCentro", c."NomeRegiao" FROM ' + tCentro + ' c) uni';
	    
	    
	    
	    
	    
    // let sQuery =  'SELECT DISTINCT uni."UnidadeCodigo" as UNID_NOME,';
    //     sQuery += ' (SELECT SUM(a."ToneladasRestantesInicial") FROM '+tProtocolo+' a WHERE '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'X\') as VOLU_AGRU,';
    //     sQuery += ' (SELECT SUM(a."ToneladasRestantesInicial")-(SELECT SUM(o."ToneladasOV") FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" o WHERE o."Centro" = uni."UnidadeCodigo" AND ' + ovFilter + ') FROM '+tProtocolo+' a WHERE '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') as VOLU_NECS,';
    //     // sQuery += ' (SELECT SUM(a."ToneladasRestantes") FROM '+tProtocolo+' a WHERE '+wUnidade+' AND ' + dateFilter + ') as VOLU_NECS,';
    //     sQuery += ' (SELECT SUM(o."ToneladasOV") FROM ' + tOrdemVenda + ' o WHERE uni."UnidadeCodigo" = o."Centro" AND ' + ovFilter + ') as VOLU_GRCM,';
    //     // sQuery += ' (SELECT (SELECT SUM(o."ToneladasOV") FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" o WHERE o."Centro" = uni."UnidadeCodigo" AND ' + ovFilter + ')/SUM(a."ToneladasRestantesInicial") From ' + tProtocolo + ' a where ' + dateFilter + ') AS PERC_CMNS,';
    //     sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE a."Status" = '+"'Complemento Total'"+' and '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') as QTDE_AGCT,'; 
    //     sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE a."Status" = '+"'Complemento Parcial'"+' and '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') as QTDE_AGCP,';
    //     sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE a."Status" = '+"'Não Complementado'"+' and '+wUnidade+' AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') as QTDE_AGIC,';
    //     sQuery += ' (SELECT COUNT(*) FROM '+tOrdemVenda+' o INNER JOIN '+tProtocolo+' a on a."NumAgrupamento" = o."NumAgrupamento"  WHERE '+wUnidade+' AND ' + ovFilter + ' AND a."Excluido" = \'\') as QTDE_PDGR,';
    //     sQuery += ' (SELECT '+cTempo+' FROM '+tProtocolo+' a WHERE '+wUnidade+' AND ' + protocoloFilter + ' AND a."Excluido" = \'\')  as TEMP_SLAA,'; 
    //     sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."UnidadeCodigo" = a."UnidadeCodigo" AND a."AtendimentoFim" <= a."SLA_OTIF" AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_OTIF,';
    //     sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."UnidadeCodigo" = a."UnidadeCodigo" AND a."AtendimentoFim" > a."SLA_OTIF" AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS FORA_OTIF,';
    //     sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."UnidadeCodigo" = a."UnidadeCodigo" AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS DENT_ATEN,';
    //     sQuery += ' (SELECT COUNT(*) FROM '+tProtocolo+' a WHERE uni."UnidadeCodigo" = a."UnidadeCodigo" AND ' + tempoAtendimento + ' > ' + SLAPadraoMinutos + ' AND ' + protocoloFilter + ' AND a."Excluido" = \'\') AS FORA_ATEN,';
    //     sQuery += ' uni."UnidadeNome" AS UNID_NOME,';
    //     sQuery += ' (SELECT SUM(a."FreteMorto") FROM ' + tProtocolo + ' a WHERE uni."UnidadeCodigo" = a."UnidadeCodigo" AND ' + protocoloDateFilter + ' AND a."Excluido" = \'\') AS VALR_FTMR,'; 
    //     sQuery += ' uni."Estado" AS UNID_ESTA,';
    //     sQuery += ' (SELECT SUM(o."Isencao") FROM ' + tOrdemVenda + ' o WHERE o."Centro" = uni."UnidadeCodigo" AND ' + ovFilter + ') AS VALR_ISEN';
    //     sQuery += ' FROM (SELECT DISTINCT p."UnidadeCodigo", p."UnidadeNome", a."Estado" FROM '+tProtocolo+' p INNER JOIN "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" a ON a."UnidadeCodigo" = p."UnidadeCodigo" WHERE a."Estado" != \'\') uni';
    
    let oStmt = oConn.prepareStatement(sQuery);
    const OTIFData = getOTIFData(sTimeInicio, sTimeFim, 'Centro');
    
    let sResultSet = oStmt.executeQuery();
    let aResponse = [];
    
    while(sResultSet.next()) {
        const VOLU_AGRU = parseFloat(parseFloat(sResultSet.getNString(2),10).toFixed(2));
        const VOLU_NECS = parseFloat(parseFloat(sResultSet.getNString(3),10).toFixed(2));
        const VOLU_GRCM = parseFloat(parseFloat(sResultSet.getNString(4),10).toFixed(2));
        const PERC_CMNS = parseFloat(parseFloat(VOLU_GRCM / VOLU_AGRU * 100).toFixed(2));
        const TEMP_SLAA = parseInt(sResultSet.getNString(9),10);
        const VALR_FTMR = parseFloat(parseFloat(sResultSet.getNString(15),10).toFixed(2));
        const VALR_ISEN = parseFloat(parseFloat(sResultSet.getNString(17),10).toFixed(2));
        
        const Centro = sResultSet.getNString(1);
        
        const DENT_OTIF = OTIFData[Centro] ? OTIFData[Centro].DENT_OTIF : 0;
		const FORA_OTIF = OTIFData[Centro] ? OTIFData[Centro].FORA_OTIF : 0;
        
        aResponse.push({
            UNID_CODE: Centro,
            UNID_NOME: sResultSet.getNString(14),
            UNID_REGI: sResultSet.getNString(16),
            VOLU_AGRU: isNaN(VOLU_AGRU) ? 0 : VOLU_AGRU,
            VOLU_NECS: isNaN(VOLU_NECS) ? 0 : VOLU_NECS,
            VOLU_GRCM: isNaN(VOLU_GRCM) ? 0 : VOLU_GRCM,
            PERC_CMNS: isNaN(PERC_CMNS) ? 0 : PERC_CMNS,
            QTDE_AGCT: parseInt(sResultSet.getNString(5),10),
            QTDE_AGCP: parseInt(sResultSet.getNString(6),10),
            QTDE_AGIC: parseInt(sResultSet.getNString(7),10),
            QTDE_PDGR: parseInt(sResultSet.getNString(8),10),
            VALR_ISEN: VALR_ISEN ? VALR_ISEN : 0,
            VALR_FTMR: VALR_FTMR ? VALR_FTMR : 0,
            TEMP_SLAA: TEMP_SLAA ? TEMP_SLAA : 0,
            DENT_OTIF: DENT_OTIF,
            FORA_OTIF: FORA_OTIF,
            DENT_ATEN: parseInt(sResultSet.getNString(12),10),
            FORA_ATEN: parseInt(sResultSet.getNString(13),10)
         });
    }
   
    return aResponse;
}

function mainFunction() {
    let sDataIni = $.request.parameters.get("DataIni");
    let sDataFim = $.request.parameters.get("DataFim");
    if(sDataIni && sDataFim) {
        let sTimeInicio = sDataIni + "T00:00:00";
        let sTimeFim = sDataFim + "T23:59:59";
        
        // let oResponse = {};
        let aResponse = [];
        
        try {
            aResponse = aResponse.concat(selectSummary(sTimeInicio, sTimeFim));
        } catch (err) {
            $.response.status = $.net.http.OK;
            $.response.contentType = "application/json";
            $.response.setBody(JSON.stringify(err));
            
            oConn.close();
            return;
        }
        
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(aResponse));
    }
    else {
        $.response.status = $.net.http.OK;
        $.response.contentType = "text/html";
        $.response.setBody("service working");
    }
}

mainFunction();
oConn.close();