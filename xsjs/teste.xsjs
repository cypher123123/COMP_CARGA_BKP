// $.response.status = $.net.http.OK;
// $.response.contentType = 'application/json';
// $.response.setBody(JSON.stringify(['39608','37919']));   

// let testNow = new Date('06/15/2020 11:00:00');

// function isNowBetweenMidnightAndInicioCelula(now, inicioCelula) {
//     const celulaHour = parseInt(inicioCelula.split(':')[0], 10);
//     const celulaMinute = parseInt(inicioCelula.split(':')[1], 10);
    
//     if(now.getHours() < celulaHour) {
//         return true;
//     }
    
//     if(now.getHours() === celulaHour && now.getMinutes() < celulaMinute ) {
//         return true;
//     }
    
//     return false;
// }

// const isBetween = isNowBetweenMidnightAndInicioCelula(testNow, '08:10');

// $.response.status = $.net.http.OK;
// $.response.contentType = 'text/html';
// $.response.setBody(isBetween.toString()); 

// let oConnection = $.db.getConnection(); 

// function getParameters() {
//     let sQuery = 'SELECT * FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros"';
//     let oStatement = oConnection.prepareStatement(sQuery);
//     let oResultSet = oStatement.executeQuery();
    
//     if(oResultSet.next()) {
//         return {
//             IdParametro: oResultSet.getInteger(1),
//             SLAPadraoMinutos: oResultSet.getDouble(2),
//             TempoCarregamentoEmMinutos: oResultSet.getInteger(3),
//             EncerramentoForcado: oResultSet.getString(4),
//             OrdemVendaSemCliente: oResultSet.getString(5)
//         };
//     }
    
//     return null;
// }

// let oParameters = getParameters();

// $.response.status = $.net.http.NOT_FOUND;
// $.response.contentType = 'application/json';
// $.response.setBody(JSON.stringify(oParameters));

// oConnection.close();



// let list = [{"NrComplemento": "03", "Cliente": "90", "OrdemVendas": "ABC"}, {"NrComplemento": "02", "Cliente": "92", "OrdemVendas": "DFG"}, {"NrComplemento": "01", "Cliente": "91", "OrdemVendas": "ZYX"}];

// let sortedList = list.sort(function(a, b) {
//     return a.NrComplemento < b.NrComplemento ? -1 : 1;
// });

// let query = 'SELECT * FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
// query += 'WHERE ';

// list.sort(function(a, b) {
//     return a.NrComplemento < b.NrComplemento ? -1 : 1;
// });

// list.forEach(function(item, i) {
//     if(i === 0) {
//         query += '"NumAgrupamento" = ? ';
//     }
//     else {
//         query += 'OR "NumAgrupamento" = ? ';
//     }
// });

// query += 'ORDER BY "NumAgrupamento"';
    
// $.response.status = $.net.http.OK;
// $.response.contentType = 'text/html';
// $.response.setBody(query);

// let oConnection = $.db.getConnection();

// function getSlaAtendimento() {
//     let sQuery = 'SELECT "SLAPadraoMinutos" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros"';
//     let oStatement = oConnection.prepareStatement(sQuery);
    
//     oStatement.executeQuery();
    
//     let oResult = oStatement.getResultSet();
    
//     if(oResult.next()) {
//         return oResult.getString(1);
//     }
// }

// function getSlaOTIF(sProtocol) {
//     let sQuery = 'SELECT "SLA_OTIF" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
//     sQuery += 'WHERE "NumAgrupamento" = ?';
    
//     let oStatement = oConnection.prepareStatement(sQuery);
//     oStatement.setString(1, sProtocol);
    
//     oStatement.executeQuery();
    
//     let oResult = oStatement.getResultSet();
    
//     if(oResult.next()) {
//         let sSla = oResult.getString(1);
//         sSla = sSla.substring(0,19);
//         sSla = sSla.replace(new RegExp(" ", 'g'), "-");
//         sSla = sSla.replace(new RegExp(":", 'g'), "-");
        
//         return sSla;
//     }
// }

// function getDateFromString(sDate) {
//     var aDate = sDate.split("-");
//     return new Date(aDate[0], aDate[1], aDate[2], aDate[3], aDate[4], aDate[5]);
// }

// function rectifyOTIF(tDate) {
//     // O SELECT em uma data no banco, devolve o mês corretamente
//     // EX: Novembro = 11. Para criar uma data JS, precisa ajustar o mês para - 1
//     return new Date(tDate.setMonth( tDate.getMonth() - 1 ));
// }

// function checkSLA(sProtocol, tDate) {
//     let sSlaAtendimento = getSlaAtendimento();
//     let sSlaOTIF = getSlaOTIF(sProtocol);
//     let tSlaOTIF = getDateFromString(sSlaOTIF);
//     tSlaOTIF = rectifyOTIF(tSlaOTIF);
    
//     let diffMs = (tSlaOTIF - tDate);
//     let diffSec = Math.ceil(diffMs / 1000);
    
//     let minTime = 60 * sSlaAtendimento; // SLA Atendimento em segundos
    
//     if(diffSec > minTime) {
//         return true;
//     }
    
//     return false;
// }

// function main() {
//     let sVendedor = "teste";
//     let sDate = "2019-10-28-15-04-00";
    
//     let sProtocol = "0001";
    
//     let tDate = getDateFromString(sDate);
    
//     let isSlaOk = checkSLA(sProtocol, tDate);
    
    
//     $.response.status = $.net.http.OK;
//     $.response.contentType = 'application/json';
//     $.response.setBody(JSON.stringify(
        
//     ));
// }

// main();
// oConnection.close();