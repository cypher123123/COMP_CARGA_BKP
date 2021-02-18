let oConnection = $.db.getConnection();

function sendResponse(aProtocols, options) {
    $.response.status = options.status;
    $.response.contentType = 'application/json';
    $.response.setBody(JSON.stringify(aProtocols)); 
}

function getQueuedProtocols() {
    const sQuery = `SELECT "NumAgrupamento" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" WHERE "Status" = 'Aguardando';`;
    
    let oStatement = oConnection.prepareStatement(sQuery);
    let oResultSet = oStatement.executeQuery();
    
    let aProtocols = [];
    
    while(oResultSet.next()) {
        aProtocols.push(oResultSet.getString(1));
    }
    
    return aProtocols;
}

try {
    const protocols = getQueuedProtocols();
    sendResponse(protocols, {
        status: 200
    });
} catch (err) {
    sendResponse(protocols, {
        status: 500
    });
}