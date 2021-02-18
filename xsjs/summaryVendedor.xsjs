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
                KPIValido: resultSet.getNString(5),
                InterfaceAtuante: resultSet.getNString(6)
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

function getVendedores(sTimeInicio, sTimeFim) {
    let tProtocolo = '"COMP_CARGA"."comp_carga.table::cds_table.Protocolo"';
	let tOrdemVenda = '"COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda"';
	let wVendedor = 'a."EmailVendedor" = v."EmailVendedor"';
	let cTempo = 'AVG(SECONDS_BETWEEN(a."AtendimentoInicio",a."AtendimentoFim") - a."PausaEmSegundos")';
	let protocoloKPIFilter = 'a."KPIValido"=\'X\'';
	let protocoloDateFilter = 'a."AtendimentoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
	let protocoloFilter = protocoloDateFilter +  ' AND ' + protocoloKPIFilter;
	let ordemVendaDateFilter = 'a."DataOrdemVenda" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
	    
	let tempoAtendimento = '(SECONDS_BETWEEN(a."AtendimentoInicio", a."AtendimentoFim") - a."PausaEmSegundos") / 60';
    let SLAPadraoMinutos = '(SELECT "SLAPadraoMinutos" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros")';
    let ovFilter = 'o."DataOrdemVenda" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
	
	let sQuery = 'SELECT v."NomeVendedor" as NOME_VEND, v."EmailVendedor" as MAIL_VEND, ';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + '  a where ' + wVendedor + ' and a."Status" = \'Complemento Total\' AND ' + protocoloDateFilter + ') as QTDE_AGCT,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + '  a where ' + wVendedor + ' and a."Status" = \'Complemento Parcial\' AND ' + protocoloDateFilter + ') as QTDE_AGCP,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + '  a where ' + wVendedor + ' and a."Status" = \'Não Complementado\' AND ' + protocoloDateFilter + ') as QTDE_AGIC,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tOrdemVenda + ' a where ' + wVendedor + ' AND ' + ordemVendaDateFilter + ') as QTDE_PDGR,';
	sQuery += ' (SELECT SUM(o."ToneladasOV") FROM ' + tOrdemVenda + ' o where o."EmailVendedor" = v."EmailVendedor" AND ' + ovFilter + ') as VOLU_ORVE,';
	sQuery += ' (SELECT ' + cTempo + ' FROM ' + tProtocolo + ' a where ' + wVendedor + ' AND ' + protocoloFilter + ') as TEMP_SLAA,';
	sQuery += ' (SELECT SUM(o."ToneladasOV") FROM ' + tOrdemVenda + ' o where ' + ovFilter + ') AS VOLU_TOTA,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + ' a WHERE a."EmailVendedor" = v."EmailVendedor" AND a."AtendimentoFim" < a."SLA_OTIF" AND ' + protocoloFilter + ') AS DENT_OTIF,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + ' a WHERE a."EmailVendedor" = v."EmailVendedor" AND a."AtendimentoFim" > a."SLA_OTIF" AND ' + protocoloFilter + ') AS FORA_OTIF,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + ' a WHERE a."EmailVendedor" = v."EmailVendedor" AND ' + tempoAtendimento + ' <= ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ') AS DENT_ATEN,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + ' a WHERE a."EmailVendedor" = v."EmailVendedor" AND ' + tempoAtendimento + ' > ' + SLAPadraoMinutos + ' AND ' + protocoloDateFilter + ') AS FORA_ATEN,';
	sQuery += ' (SELECT SUM(a."Isencao") FROM ' + tOrdemVenda + ' a WHERE a."EmailVendedor" = v."EmailVendedor" AND ' + ordemVendaDateFilter + ') AS VALR_ISEN,';
	sQuery += ' (SELECT SUM(a."FreteMortoEvitado") FROM ' + tOrdemVenda + ' a WHERE a."EmailVendedor" = v."EmailVendedor" AND ' + ordemVendaDateFilter + ') AS VALR_FTME,';
	sQuery += ' (SELECT SUM(a."FreteMorto") FROM ' + tProtocolo + ' a WHERE a."EmailVendedor" = v."EmailVendedor" AND ' + protocoloDateFilter + ') AS VALR_FTMR';
	sQuery += ' FROM "COMP_CARGA"."comp_carga.table::cds_table.Vendedor" v ';
	sQuery += ' WHERE v."Excluido" != \'X\' ';

	let oStmt = oConn.prepareStatement(sQuery);

	let sResultSet = oStmt.executeQuery();
	const OTIFData = getOTIFData(sTimeInicio, sTimeFim, 'EmailVendedor');
	
	let aResponse = [];

	while (sResultSet.next()) {
        const VOLU_ORVE = parseFloat(parseFloat(sResultSet.getNString(7)).toFixed(2));
        const TEMP_SLAA = parseInt(parseInt(sResultSet.getNString(8), 10) / 60, 10);
        const VOLU_TOTA = parseFloat(sResultSet.getNString(9));
        const PERC_CMNS = parseFloat(parseFloat(VOLU_ORVE / VOLU_TOTA * 100).toFixed(2));
        const VALR_ISEN = parseFloat(parseFloat(sResultSet.getNString(14)).toFixed(2));
        const VALR_FTMR = parseFloat(parseFloat(sResultSet.getNString(16)).toFixed(2));
        const VALR_FTME = parseFloat(parseFloat(sResultSet.getNString(15)).toFixed(2));
        
        const EmailVendedor = sResultSet.getNString(2);
        
		const DENT_OTIF = OTIFData[EmailVendedor] ? OTIFData[EmailVendedor].DENT_OTIF : 0;
		const FORA_OTIF = OTIFData[EmailVendedor] ? OTIFData[EmailVendedor].FORA_OTIF : 0;
        
		aResponse.push({
			NOME_VEND: sResultSet.getNString(1),
			MAIL_VEND: EmailVendedor,
			QTDE_AGCT: parseInt(sResultSet.getNString(3), 10),
			QTDE_AGCP: parseInt(sResultSet.getNString(4), 10),
			QTDE_AGIC: parseInt(sResultSet.getNString(5), 10),
			QTDE_PDGR: parseInt(sResultSet.getNString(6), 10),
			VOLU_ORVE: isNaN(VOLU_ORVE) ? 0 : VOLU_ORVE,
			TEMP_SLAA: TEMP_SLAA ? TEMP_SLAA : 0,
			PERC_CMNS: isNaN(PERC_CMNS) ? 0 : PERC_CMNS,
			DENT_OTIF: DENT_OTIF,
			FORA_OTIF: FORA_OTIF,
// 			DENT_OTIF: parseInt(sResultSet.getNString(10), 10),
// 			FORA_OTIF: parseInt(sResultSet.getNString(11), 10),
			DENT_ATEN: parseInt(sResultSet.getNString(12), 10),
			FORA_ATEN: parseInt(sResultSet.getNString(13), 10),
			VALR_ISEN: VALR_ISEN ? VALR_ISEN : 0,
			VALR_FTMR: VALR_FTMR ? VALR_FTMR : 0,
			VALR_FTME: VALR_FTME ? VALR_FTME : 0
		});
	}

	return aResponse;
}

function getInterface(sTimeInicio, sTimeFim) {
    let tProtocolo = '"COMP_CARGA"."comp_carga.table::cds_table.Protocolo"';
	let tOrdemVenda = '"COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda"';
	let cTempo = 'AVG(SECONDS_BETWEEN(a."AtendimentoInicio",a."AtendimentoFim") - a."PausaEmSegundos")';
	let protocoloDateFilter = 'a."AtendimentoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
	
    let sQuery = 'SELECT p."InterfaceAtuante" as NOME_VEND,';
    sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + '  a where p."InterfaceAtuante" = a."InterfaceAtuante" AND a."Status" = \'Complemento Total\' AND ' + protocoloDateFilter + ') as QTDE_AGCT,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + '  a where p."InterfaceAtuante" = a."InterfaceAtuante" AND a."Status" = \'Complemento Parcial\' AND ' + protocoloDateFilter + ') as QTDE_AGCP,';
	sQuery += ' (SELECT COUNT(*) FROM ' + tProtocolo + '  a where p."InterfaceAtuante" = a."InterfaceAtuante" AND a."Status" = \'Não Complementado\' AND ' + protocoloDateFilter + ') as QTDE_AGIC,';
	sQuery += ' (SELECT ' + cTempo + ' FROM ' + tProtocolo + ' a where p."InterfaceAtuante" = a."InterfaceAtuante" AND ' + protocoloDateFilter + ') as TEMP_SLAA,';
	sQuery += ' (SELECT SUM(a."FreteMorto") FROM ' + tProtocolo + ' a WHERE p."InterfaceAtuante" = a."InterfaceAtuante" AND ' + protocoloDateFilter + ') AS VALR_FTMR,';
	sQuery += ' (SELECT SUM(o."ToneladasOV") FROM ' + tOrdemVenda + ' o WHERE o."DataOrdemVenda" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\') AS VOLU_TOTA';
	sQuery += ' FROM (SELECT DISTINCT "InterfaceAtuante" FROM ' + tProtocolo + ' WHERE "Excluido" = \'\' AND "EmailVendedor" = \'\' AND NOT "InterfaceAtuante" = \'\') p';

	let oStmt = oConn.prepareStatement(sQuery);
	const OTIFData = getOTIFData(sTimeInicio, sTimeFim, 'InterfaceAtuante');

	let sResultSet = oStmt.executeQuery();
	let aResponse = [];

	while (sResultSet.next()) {
        const TEMP_SLAA = parseInt(parseInt(sResultSet.getNString(5), 10) / 60, 10);
        const VALR_FTMR = parseFloat(parseFloat(sResultSet.getNString(6)).toFixed(2));
        
        let NOME_VEND = sResultSet.getNString(1);
        
        const DENT_OTIF = OTIFData[NOME_VEND] ? OTIFData[NOME_VEND].DENT_OTIF : 0;
		const FORA_OTIF = OTIFData[NOME_VEND] ? OTIFData[NOME_VEND].FORA_OTIF : 0;
        
		aResponse.push({
			NOME_VEND: NOME_VEND,
			QTDE_AGCT: parseInt(sResultSet.getNString(2), 10),
			QTDE_AGCP: parseInt(sResultSet.getNString(3), 10),
			QTDE_AGIC: parseInt(sResultSet.getNString(4), 10),
			VOLU_TOTA: parseInt(sResultSet.getNString(7), 10),
			TEMP_SLAA: TEMP_SLAA ? TEMP_SLAA : 0,
			DENT_OTIF: DENT_OTIF,
			FORA_OTIF: FORA_OTIF,
			DENT_ATEN: 0,
			FORA_ATEN: 0,
			VALR_FTMR: VALR_FTMR ? VALR_FTMR : 0
		});
	}

	return aResponse;
}

function getOVData(sTimeInicio, sTimeFim){
    let tProtocolo = '"COMP_CARGA"."comp_carga.table::cds_table.Protocolo"';
	let tOrdemVenda = '"COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda"';
	let protocoloDateFilter = '"AtendimentoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
	let ordemVendaDateFilter = 'o."DataOrdemVenda" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
    let ovFilter = 'o."DataOrdemVenda" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
    
    let query = '   SELECT ';
    query +=    '       Protocolo."NumAgrupamento", ';
    query +=    '       Protocolo."InterfaceAtuante", ';
    // query +=    '       (Protocolo."ToneladasRestantesInicial" - Protocolo."ToneladasRestantes")                                                                                AS VOLU_ORVE, ';
    query +=    '       (SELECT COUNT(*)                    FROM ' + tOrdemVenda + ' o WHERE Protocolo."NumAgrupamento" = o."NumAgrupamento" AND ' + ordemVendaDateFilter + ')  AS QTDE_PDGR, ';
    query +=    '       (SELECT SUM(o."ToneladasOV")        FROM ' + tOrdemVenda + ' o WHERE Protocolo."NumAgrupamento" = o."NumAgrupamento" AND ' + ovFilter + ')              AS VOLU_ORVE, ';
    query +=    '       (SELECT SUM(o."Isencao")            FROM ' + tOrdemVenda + ' o WHERE Protocolo."NumAgrupamento" = o."NumAgrupamento" AND ' + ordemVendaDateFilter + ')  AS VALR_ISEN, ';
    query +=    '       (SELECT SUM(o."FreteMortoEvitado")  FROM ' + tOrdemVenda + ' o WHERE Protocolo."NumAgrupamento" = o."NumAgrupamento" AND ' + ordemVendaDateFilter + ')  AS VALR_FTME ';
    query +=    '   FROM (SELECT * FROM ' + tProtocolo + ' ';
    query +=    '   WHERE "Excluido" = \'\' AND "EmailVendedor" = \'\' AND NOT "InterfaceAtuante" = \'\' AND ' + protocoloDateFilter + ') Protocolo; ';
    
	let oStmt = oConn.prepareStatement(query);

	let sResultSet = oStmt.executeQuery();
	let aProtocols = [];

	while (sResultSet.next()) {
	    aProtocols.push({
	        NumAgrupamento: sResultSet.getNString(1),
	        InterfaceAtuante: sResultSet.getNString(2),
	        QTDE_PDGR: sResultSet.getNString(3),
    	    VOLU_ORVE: parseFloat(parseFloat(sResultSet.getNString(4)).toFixed(2)),
    	    VALR_ISEN: parseFloat(parseFloat(sResultSet.getNString(5)).toFixed(2)),
    	    VALR_FTME: parseFloat(parseFloat(sResultSet.getNString(6)).toFixed(2))
	    });
	}

	return aProtocols;
}

function selectSummary(sTimeInicio, sTimeFim) {
	const vendedoresData = getVendedores(sTimeInicio, sTimeFim);
	
	const interfaceData = getInterface(sTimeInicio, sTimeFim);
	const ovData = getOVData(sTimeInicio, sTimeFim, interfaceData);
	
	const aInterfaceAtuantes = interfaceData.reduce(function (accInterfaceData, item) {
	    const aOVItems = ovData.filter(function (ov) {
	        return ov.InterfaceAtuante === item.NOME_VEND;
	    });
	    
	    /* eslint-disable */
    	const oOVItemsSum = aOVItems.reduce(function (accOVItems, data) {
    	    if (!accOVItems.QTDE_PDGR) accOVItems.QTDE_PDGR = 0;
    	    if (!accOVItems.VOLU_ORVE) accOVItems.VOLU_ORVE = 0;
    	    if (!accOVItems.VALR_ISEN) accOVItems.VALR_ISEN = 0;
    	    if (!accOVItems.VALR_FTME) accOVItems.VALR_FTME = 0;
    	    
    	    accOVItems.QTDE_PDGR += parseFloat(data.QTDE_PDGR) ? parseFloat(data.QTDE_PDGR) : 0
    	    accOVItems.VOLU_ORVE += parseFloat(data.VOLU_ORVE) ? parseFloat(data.VOLU_ORVE) : 0;
    	    accOVItems.VALR_ISEN += parseFloat(data.VALR_ISEN) ? parseFloat(data.VALR_ISEN) : 0;
    	    accOVItems.VALR_FTME += parseFloat(data.VALR_FTME) ? parseFloat(data.VALR_FTME) : 0;
    	    
    	    return accOVItems;
    	}, {})
    	/* eslint-enable */
    	
    	if (aOVItems.length === 0) {
    	    item.QTDE_PDGR = 0;
    	    item.VOLU_ORVE = 0;
    	    item.VALR_ISEN = 0;
    	    item.VALR_FTME = 0;
    	}
    	
    	if (item.NOME_VEND === 'Encerramento' || item.NOME_VEND === 'Ordem de Venda') {
            item.NOME_VEND = 'Automático';
        }
        
        item.PERC_CMNS = parseFloat(parseFloat((oOVItemsSum.VOLU_ORVE ? oOVItemsSum.VOLU_ORVE : 0) / item.VOLU_TOTA * 100).toFixed(2));
	    
	    accInterfaceData.push(Object.assign(item, oOVItemsSum));
	    
	    return accInterfaceData;
	}, []);
	
	const sumInterfaceAtuantes = aInterfaceAtuantes.reduce(function (acc, item) {
	    acc.NOME_VEND = item.NOME_VEND;
		acc.QTDE_AGCT += item.QTDE_AGCT;
		acc.QTDE_AGCP += item.QTDE_AGCP;
		acc.QTDE_AGIC += item.QTDE_AGIC;
		acc.VOLU_TOTA += item.VOLU_TOTA;
		acc.DENT_OTIF += 0;
		acc.FORA_OTIF += 0;
		acc.DENT_ATEN += 0;
		acc.FORA_ATEN += 0;
		acc.VALR_FTMR += item.VALR_FTMR;
		acc.QTDE_PDGR += item.QTDE_PDGR;
	    acc.VOLU_ORVE += item.VOLU_ORVE;
	    acc.VALR_ISEN += item.VALR_ISEN;
	    acc.VALR_FTME += item.VALR_FTME;
	    
	    return acc;
	});
	
    sumInterfaceAtuantes.TEMP_SLAA += aInterfaceAtuantes.reduce(function (acc, item) { return acc + item.TEMP_SLAA / aInterfaceAtuantes.length; }, 0);
	
	vendedoresData.push(sumInterfaceAtuantes);
	
	return vendedoresData;
}

function mainFunction() {
	let sDataIni = $.request.parameters.get("DataIni");
	let sDataFim = $.request.parameters.get("DataFim");
	if (sDataIni && sDataFim) {
		let sTimeInicio = sDataIni + "T00:00:00";
		let sTimeFim = sDataFim + "T23:59:59";
		
		let aResponse = [];

		aResponse = aResponse.concat(selectSummary(sTimeInicio, sTimeFim));

		$.response.status = $.net.http.OK;
		$.response.contentType = "application/json";
		$.response.setBody(JSON.stringify(aResponse));
	} else {
		$.response.status = $.net.http.OK;
		$.response.contentType = "text/html";
		$.response.setBody("service working");
	}
}

mainFunction();
oConn.close();