let oConn = $.db.getConnection();
/*
Volume Total    : VOLU_TOTA     OK
Volume Vendido  : VOLU_VEND     OK 
Volume Não Vendido : VOLU_NVEN  OK
*/

function selectSummary(sTimeInicio, sTimeFim) {
    let ligacaoFilter = '(Ligacao."LigacaoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\') AND (Ligacao."LigacaoInicio" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\')';
    let ligFilter = '(Lig."LigacaoFim" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\') AND (Lig."LigacaoInicio" BETWEEN \'' + sTimeInicio + '\' AND \'' + sTimeFim + '\')';
    let ligacao = '"COMP_CARGA"."comp_carga.table::cds_table.Ligacoes"';
    let notCompleted = '(Ligacao."Resultado" != \'Complemento Total\' AND Ligacao."Resultado" != \'Complemento Parcial\')';
// 	let sQuery = '' +
// 	' SELECT' +
//     ' COUNT(' +
//     '   CASE WHEN Ligacoes."Resultado" = \'Estoque Cheio\' THEN 1 ELSE NULL END' +
//     ' ) AS ESTO_CHEI,' +
//     ' COUNT(' +
//     '   CASE WHEN Ligacoes."Resultado" = \'Não Aceita Complemento\' THEN 1 ELSE NULL END' +
//     ' ) AS NACE_COMP,' +
//     ' COUNT(' +
//     '   CASE WHEN Ligacoes."Resultado" = \'Condições Cimáticas\' THEN 1 ELSE NULL END' +
//     ' ) AS COND_CLIM,' +
//     ' COUNT(' +
//     '   CASE WHEN Ligacoes."Resultado" = \'Problema de Crédito\' THEN 1 ELSE NULL END' +
//     ' ) AS PROB_CRED' +
//     ' FROM "COMP_CARGA"."comp_carga.table::cds_table.Ligacoes" AS Ligacoes WHERE ' + ligacaoFilter + ' ';
    
    let query = ' SELECT Ligacao."Resultado" AS STAT_NOME,';
    query +=    ' (SELECT COUNT(*) FROM "COMP_CARGA"."comp_carga.table::cds_table.Ligacoes" Lig WHERE Lig."Resultado" = Ligacao."Resultado" AND ' + ligFilter + ') AS STAT_QTDE';
    query +=    ' FROM (SELECT DISTINCT Ligacao."Resultado" FROM ' + ligacao + ' AS Ligacao WHERE ' + ligacaoFilter + ' AND ' + notCompleted + ') AS Ligacao';

	let oStmt = oConn.prepareStatement(query);

	let ResultSet = oStmt.executeQuery();
	let results = [];
	
	while (ResultSet.next()) {
	    
	    results.push({
	       STAT_NOME:  ResultSet.getNString(1),
	       STAT_QTDE:  parseInt(ResultSet.getNString(2), 10)
	    });
        
// 		return {
// 		    ESTO_CHEI: parseInt(ResultSet.getNString(1), 10),
// 		    NACE_COMP: parseInt(ResultSet.getNString(2), 10),
// 		    COND_CLIM: parseInt(ResultSet.getNString(3), 10),
// 		    PROB_CRED: parseInt(ResultSet.getNString(4), 10)
// 		};
	}
	
	return results
	    .filter(function (result) {
	        return Boolean(result.STAT_NOME);
	    })
	    .map(function (result) {
	        switch (result.STAT_NOME) {
	            case 'DialingCanceled':
	                return Object.assign(result, { STAT_NOME: 'Cancelado' });
                default:
                    return result;
	        }
	    })
	    .sort(function (a, b) {
	        return b.STAT_QTDE - a.STAT_QTDE;
	    });
// 	return { ESTO_CHEI: 0, NACE_COMP: 0, COND_CLIM: 0, PROB_CRED: 0 };
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