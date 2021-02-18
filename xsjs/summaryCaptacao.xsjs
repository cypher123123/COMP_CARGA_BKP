let oConn = $.db.getConnection();
/*
Volume Total    : VOLU_TOTA     OK
Volume Vendido  : VOLU_VEND     OK 
Volume Não Vendido : VOLU_NVEN  OK
*/

function selectSummary(sTimeInicio, sTimeFim) {
    let protocoloDateFilter = 'Protocolo."SLA_OTIF" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
// 	let ordemVendaDateFilter = 'OrdemVenda."DataOrdemVenda" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\'';
// 	let onGoingProtocol = '(Protocolo."Status" = \'Aguardando\' OR Protocolo."Status" = \'Pausada\' OR Protocolo."Status" = \'Em Atendimento\') ';
	    
	    
// 	let sQuery = '';
// 	sQuery += 'SELECT SUM(Protocolo."ToneladasRestantesInicial") AS VOLU_TOTA, ';
// 	sQuery += '(';
// 	sQuery += '     SELECT SUM(OrdemVenda."ToneladasOV") ';
// 	sQuery += '	    FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" AS OrdemVenda WHERE ' + ordemVendaDateFilter;
// 	sQuery += ') AS VOLU_VEND, ';
// 	sQuery += '(';
// 	sQuery += '	    SELECT SUM(Protocolo."ToneladasRestantesInicial") - SUM(OrdemVenda."ToneladasOV") ';
// 	sQuery += '	    FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" AS OrdemVenda WHERE ' + ordemVendaDateFilter;
// 	sQuery += ') AS VOLU_DIFF ';
//     sQuery += 'FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" AS Protocolo WHERE ' + protocoloDateFilter;
    
    let query = '';
    query += ' SELECT SUM(Protocolo."ToneladasRestantesInicial") AS VOLU_TOTA, ';
    query += ' (SUM(Protocolo."ToneladasRestantesInicial") - SUM(Protocolo."ToneladasRestantes")) AS VOLU_VEND ';
    query += ' FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" AS Protocolo WHERE ' + protocoloDateFilter + ' AND "Excluido" = \'\' ';

	let oStmt = oConn.prepareStatement(query);

	let sResultSet = oStmt.executeQuery();

	while (sResultSet.next()) {
        const VOLU_TOTA = parseFloat(parseFloat(sResultSet.getNString(1)).toFixed(2));
        const VOLU_VEND = parseFloat(parseFloat(sResultSet.getNString(2)).toFixed(2));
        const VOLU_DIFF = parseFloat(parseFloat(VOLU_TOTA - VOLU_VEND).toFixed(2));
        
		return {
		    VOLU_TOTA: VOLU_TOTA ? VOLU_TOTA : 0,
		    VOLU_VEND: VOLU_VEND ? VOLU_VEND : 0,
		    VOLU_DIFF: VOLU_DIFF ? VOLU_DIFF : 0
		};
	}

	return { VOLU_TOTA: 0, VOLU_VEND: 0, VOLU_DIFF: 0 };
}
 
function mainFunction() {
	let sDataIni = $.request.parameters.get("DataIni");
	let sDataFim = $.request.parameters.get("DataFim");
	if (sDataIni && sDataFim) {
		let sTimeInicio = sDataIni + "T00:00:00";
		let sTimeFim = sDataFim + "T23:59:59";

		let response = selectSummary(sTimeInicio, sTimeFim);

		$.response.status = $.net.http.OK;
		$.response.contentType = "application/json";
		$.response.setBody(JSON.stringify(response));
	} else {
		$.response.status = $.net.http.OK;
		$.response.contentType = "text/html";
		$.response.setBody("service working");
	}
}

mainFunction();
oConn.close();