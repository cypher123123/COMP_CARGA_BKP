let oConnection = $.db.getConnection();

function executeDelete(query, protocols) {
    try {
        let stmt = oConnection.prepareStatement(query);
        stmt.setBatchSize(protocols.length);
        
        for(let index in protocols) {
            stmt.setString(1, protocols[index]);
            stmt.addBatch();
        }
        
        stmt.executeUpdate();
        oConnection.commit();
        
    } catch(e) {
        return null;
    }
}

function deleteProtocolo(protocols) {
    const query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" WHERE "NumAgrupamento" = ?';
    
    executeDelete(query, protocols);
}

function deleteProtocoloCliente(protocols) {
    const query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloCliente" WHERE "NumAgrupamento" = ?';
    
    executeDelete(query, protocols);
}

function deleteOrdensVenda(protocols) {
    const query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" WHERE "NumAgrupamento" = ?';
    
    executeDelete(query, protocols);
}

function deleteUltimasComprasHeader(protocols) {
    const query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.UltimasComprasHeader" WHERE "NumAgrupamento" = ?';
    
    executeDelete(query, protocols);
}

function deleteUltimasComprasItens(protocols) {
    const query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.UltimasComprasItens" WHERE "NumAgrupamento" = ?';
    
    executeDelete(query, protocols);
}

function deleteLogOlderThanOneMonth() {
    let logConnection = $.db.getConnection();
    let query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.LOG_ResetedProtocols" WHERE "Time" < ?';
    let lastMonth = new Date(new Date().setDate(new Date().getDate() - 30));

    try {
        let stmt = logConnection.prepareStatement(query);
        stmt.setTimestamp(1, lastMonth);
        stmt.executeUpdate();
        logConnection.commit();
    } catch(e) {
        return;
    }
    
    logConnection.close();
}

function saveLog(protocols) {
    deleteLogOlderThanOneMonth();
    
    let logConnection = $.db.getConnection();
    let query = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_ResetedProtocols" ("Time", "Input") VALUES(?, ?)';
    const now = new Date();

    try {
        let stmt = logConnection.prepareStatement(query);
        stmt.setTimestamp(1, now);
        stmt.setString(2, protocols ? JSON.stringify(protocols) : null);
        stmt.executeUpdate();
        logConnection.commit();
    } catch(e) {
        return;
    }
    
    logConnection.close();
}

function sendResponse(aProtocols) {
    if(!aProtocols) {
        $.response.status = $.net.http.NO_CONTENT;
        $.response.contentType = 'application/json';
        $.response.setBody();    
        return;
    }
    
    $.response.status = $.net.http.OK;
    $.response.contentType = 'application/json';
    $.response.setBody(JSON.stringify(aProtocols)); 
}

function mainFunction() {
    let params = $.request.parameters.get("complementos");
    const queuedProtocols = params.split(";");
    
    deleteProtocolo(queuedProtocols);
    deleteProtocoloCliente(queuedProtocols);
    deleteOrdensVenda(queuedProtocols);
    deleteUltimasComprasHeader(queuedProtocols);
    deleteUltimasComprasItens(queuedProtocols);
    
    saveLog(queuedProtocols);
    sendResponse(queuedProtocols);
}

mainFunction();
oConnection.close();