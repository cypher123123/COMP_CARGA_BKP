let oConnection = $.db.getConnection();

function getNotFinishedProtocols() {
    let sQuery = `SELECT "NumAgrupamento" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"
                  WHERE "Status" = 'Aguardando' OR "Status" = 'Em Atendimento' OR "Status" = 'Pausada';`;
    
    let aProtocols = [];
    
    try {
        let oStatement = oConnection.prepareStatement(sQuery);
        let oResultSet = oStatement.executeQuery();
        
        while(oResultSet.next()) {
            aProtocols.push({
                NrComplemento: oResultSet.getString(1)
            });
        }    
    } catch(e) {
        return null;
    }
    
    return aProtocols;
}

function sendResponse(aProtocols) {
    if(!aProtocols) {
        $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
        $.response.contentType = 'application/json';
        $.response.setBody();    
        return;
    }
    
    $.response.status = $.net.http.OK;
    $.response.contentType = 'application/json';
    $.response.setBody(JSON.stringify(aProtocols)); 
}

function mainFunction() {
    let notFinishedProtocols = getNotFinishedProtocols();
    sendResponse(notFinishedProtocols);
}

mainFunction();
oConnection.close();