// const oConn = $.db.getConnection();

// function getProtocolsToDelete (sNumAgrupamento, sIdCliente) {
//     let query = 'SELECT ProtocoloCliente."NumAgrupamento" FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloCliente" AS ProtocoloCliente ';
//     query +=    'JOIN "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" AS Protocolo ';
//     query +=    'ON ProtocoloCliente."NumAgrupamento" = Protocolo."NumAgrupamento" ';
//     query +=    'WHERE (Protocolo."Status" = ? OR Protocolo."Status" = ?) ';
//     query +=    '   AND (NOT ProtocoloCliente."NumAgrupamento" = ? AND ProtocoloCliente."IdCliente" = ?) ';
//     query +=    'ORDER BY ProtocoloCliente."NumAgrupamento" ASC';
    
//     const oStatement = oConn.prepareStatement(query);
    
//     oStatement.setNString(1, 'Pausada');
//     oStatement.setNString(2, 'Aguardando');
//     oStatement.setNString(3, sNumAgrupamento);
//     oStatement.setNString(4, sIdCliente);
    
//     const oResultSet = oStatement.executeQuery();
//     let aResult = [];
    
//     while (oResultSet.next()) {
//         aResult.push(oResultSet.getNString(1));
//     }
    
//     return aResult;
// }

// function deleteCustomers (aNumAgrupamento, sIdCliente) {
//     /* eslint-disable curly */
//     if (!aNumAgrupamento.length) return 0;
    
//     let query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloCliente" ';
//     query +=    'WHERE "NumAgrupamento" = ? AND "IdCliente" = ?';
    
//     const oStatement = oConn.prepareStatement(query);
    
//     oStatement.setBatchSize(aNumAgrupamento.length);
    
//     aNumAgrupamento.forEach(function (sNumAgrupamento) {
//         oStatement.setNString(1, sNumAgrupamento);
//         oStatement.setNString(2, sIdCliente);
        
//         oStatement.addBatch();
//     });
    
//     const aRecords = oStatement.executeBatch();
    
//     oConn.commit();
    
//     return aRecords.reduce(function (acc, iRecord) {
//         acc += iRecord;
//         return acc;
//     }, 0);
// }

// function mainFunction () {
//     const sNumAgrupamento = $.request.parameters.get('NumAgrupamento');
//     const sIdCliente = $.request.parameters.get('IdCliente');
    
//     if (!sNumAgrupamento || !sIdCliente) {
//         $.response.status = $.net.http.OK;
//         $.response.contentType = "text/html";
//         $.response.setBody("No 'sNumAgrupamento' nor 'sIdCliente' provided");
//         return;
//     }
    
//     try {
        
//         const aDeleteFrom = getProtocolsToDelete(sNumAgrupamento, sIdCliente);
//         const iDeleted = deleteCustomers(aDeleteFrom, sIdCliente);
        
//         $.response.status = $.net.http.OK;
//         $.response.contentType = "text/html";
//         $.response.setBody(JSON.stringify({
//             'Registros deletados': iDeleted
//         }));
//         // $.response.setBody(JSON.stringify(aDeleteFrom));
        
//     } catch (e) {
//         $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
//         $.response.contentType = "text/html";
//         $.response.setBody("Error: " + e.message);
//     }
    
// }

// mainFunction();