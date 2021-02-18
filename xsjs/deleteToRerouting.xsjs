// let oConn = $.db.getConnection();

// function removeProtocols(aProtocols) {
//     if(aProtocols.length === 0) {
//         return;
//     }
    
//     let sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
//     sQuery += 'SET "Excluido" = \'X\', "InterfaceAtuante" = \'Re-roteirização\'';
//     sQuery += 'WHERE "NumAgrupamento" = ?';
    
//     try {
//         let oStmt = oConn.prepareStatement(sQuery);
//         oStmt.setBatchSize(aProtocols.length);
        
//         aProtocols.forEach(function(oProtocol) {
//             oStmt.setString(1, oProtocol.NumAgrupamento.toString());
//         	oStmt.addBatch();
//     	});
        
//     	let aRecords = oStmt.executeBatch();
//         oConn.commit();
        
//         let iDeletedProtocols = 0;
        
//         aRecords.forEach(function(iRecord) {
//             iDeletedProtocols += iRecord;
//         });
    	
//     } catch(e) {
//         oConn.close();
//     }
// }

// function getProtocolsToDelete() {
// 	let query = 'SELECT "NumAgrupamento", "Status", "Excluido", "InterfaceAtuante","ToneladasRestantes", "ToneladasRestantesInicial", "MinimaToneladasRestante" ';
// 	query += 'FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
// 	query += 'WHERE ("SLA_OTIF" BETWEEN ? AND ?)';
// 	query += 'AND ("Status" = \'Complemento Parcial\' OR "Status" = \'Não Complementado\') ';
// 	query += 'AND NOT ("Excluido" = \'X\')';

// 	const oStatement = oConn.prepareStatement(query);

//     let start = new Date(new Date().setHours(new Date().getHours() - 3));
//     start.setDate(start.getDate() + 2);
//     let end = new Date(new Date(start).setDate(start.getDate() + 1));

// 	oStatement.setTimestamp(1, start);
// 	oStatement.setTimestamp(2, end);

// 	const oResult = oStatement.executeQuery();
// 	let aProtocols = [];

// 	while (oResult.next()) {
// 		aProtocols.push({
// 			NumAgrupamento: oResult.getString(1),
// 			Status: oResult.getString(2),
// 			Excluido: oResult.getString(3),
// 			InterfaceAtuante: oResult.getString(4),
// 			ToneladasRestantes: oResult.getString(5),
// 			ToneladasRestantesInicial: oResult.getString(6),
// 			MinimaToneladasRestante: oResult.getString(7),
// 			Delete: 'X'
// 		});
// 	}

// 	return aProtocols;
// }

// function deleteProtocols () {
//     let aProtocolsDelete = getProtocolsToDelete();
//     removeProtocols(aProtocolsDelete);
    
//     return aProtocolsDelete;
// }

// function getStatus (oProtocol) {
//     const ToneladasRestantes = parseFloat(oProtocol.ToneladasRestantes);
//     const ToneladasRestantesInicial = parseFloat(oProtocol.ToneladasRestantesInicial);
//     const MinimaToneladasRestante = parseFloat(oProtocol.MinimaToneladasRestante);
    
//     if (isNaN(ToneladasRestantes)) {
//         return '';
//     } else if (ToneladasRestantes === ToneladasRestantesInicial) {
//         return 'Não Complementado';
//     } else if (ToneladasRestantes <= MinimaToneladasRestante) {
//         return 'Complemento Total';
//     } else {
//         return 'Complemento Parcial';
//     }
// }

// function getProtocolsSalesOrder (aProtocols) {
//     let sQuery =  'SELECT "NumAgrupamento"';
//     sQuery += 'FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" ';
//     sQuery += 'WHERE "NumAgrupamento" = ? AND NOT "OrdemDoComplemento" = 1';
    
//     let aSalesOrder = [];
//     aProtocols.forEach(function (oProtocol) {
//         let oStmt = oConn.prepareStatement(sQuery);
//         oStmt.setString(1, oProtocol.NumAgrupamento.toString());
        
//         let oResultSet = oStmt.executeQuery();
        
        
//         while (oResultSet.next()) {
//             aSalesOrder.push(oResultSet.getString(1));
//         }
//     });
    
//     return aSalesOrder;
// }

// function getResponseData (aProtocols) {
//     let aSalesOrder = getProtocolsSalesOrder(aProtocols);
    
//     return {
//         Dummy: '',
//         'ProtocolosVencidosSet': aProtocols.map(function (oProtocol) {
//             let aProtocolOrders = aSalesOrder.filter(function (order) {
//                 return order.toString() === oProtocol.NumAgrupamento.toString();
//             });
            
//             return {
//                 NrComplemento: oProtocol.NumAgrupamento.toString(),
//                 Status: getStatus(oProtocol),
//                 houveOrdem: aProtocolOrders.length ? 'X' : '',
//                 Excluido: oProtocol.Delete ? 'X' : ''
//             };
//         })
//     };
// }

// function deleteLogOlderThanOneMonth() {
//     let logConnection = $.db.getConnection();
//     let query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.LOG_DeleteToRerouting" WHERE "Time" < ?';
//     let lastMonth = new Date(new Date().setDate(new Date().getDate() - 30));

//     try {
//         let stmt = logConnection.prepareStatement(query);
//         stmt.setTimestamp(1, lastMonth);
//         stmt.executeUpdate();
//         logConnection.commit();
//     } catch(e) {
//         return;
//     }
    
//     logConnection.close();
// }

// function saveLog(protocols) {
//     deleteLogOlderThanOneMonth();
    
//     let logConnection = $.db.getConnection();
//     let query = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_DeleteToRerouting" ("Time", "Input") VALUES(?, ?)';
//     const now = new Date(new Date().setHours(new Date().getHours() - 3));

//     try {
//         let stmt = logConnection.prepareStatement(query);
//         stmt.setTimestamp(1, now);
//         stmt.setString(2, protocols ? JSON.stringify(protocols) : null);
//         stmt.executeUpdate();
//         logConnection.commit();
//     } catch(e) {
//         return;
//     }
    
//     logConnection.close();
// }

// function mainFunction() {
//     try {
//         const aDeletedProtocols = deleteProtocols();
//         const response = getResponseData(aDeletedProtocols);
//         saveLog(response);
        
//         $.response.status = $.net.http.OK;
//         $.response.contentType = "application/json";
//         $.response.setBody(JSON.stringify(response));
    
//     } catch(e) {
//         $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
//         $.response.contentType = "text/html";
//         $.response.setBody( e.message );
//     }
    
// }

// mainFunction();
// oConn.close();