let oConnection = $.db.getConnection();
// now tem o valor do gmt brasilia
let now = new Date(new Date().setHours(new Date().getHours() - 3));

function getUnfinishedProtocol(emailVendedor) {
    let sQuery = 'SELECT "NumAgrupamento", "AtendimentoInicio" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery +=    'WHERE NOT "Excluido" = ? AND "EmailVendedor" = ? AND "Status" = ?';
    
    let oStatement = oConnection.prepareStatement(sQuery);
    oStatement.setString(1, 'X');
    oStatement.setString(2, emailVendedor);
    oStatement.setString(3, 'Em Atendimento');
    
    let oResult = oStatement.executeQuery();
    
    if(oResult.next()) {
        return {
            NumAgrupamento: oResult.getString(1),
            AtendimentoInicio: oResult.getTimestamp(2)
        };
    }
    
    return null;
}

function getNextProtocol() {
    let sQuery = 'SELECT TOP 1 "NumAgrupamento", "SLA_OTIF" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    // Início da Alteração - Alexandre Passarelli - INC0286022 
    // sQuery +=    'WHERE NOT "Excluido" = ? AND "Status" = ? ORDER BY "SLA_OTIF" ASC';
     sQuery +=    'WHERE NOT "Excluido" = ? AND "Status" = ? ORDER BY DAYOFYEAR(SLA_OTIF) DESC, HOUR(SLA_OTIF) ASC,  MINUTE(SLA_OTIF) ASC';
    // Fim da Alteração - Alexandre Passarelli - INC0286022
     
    let oStatement = oConnection.prepareStatement(sQuery);
    oStatement.setString(1, 'X');
    oStatement.setString(2, 'Aguardando');
    
    let oResult = oStatement.executeQuery();
    
    if(oResult.next()) {
        return {
            NumAgrupamento: oResult.getString(1),
            SLA_OTIF: oResult.getTimestamp(2)
        };
    }
    
    return null;
}

function getParameters() {
    let sQuery = 'SELECT "IdParametro", "SLAPadraoMinutos", "TempoCarregamentoEmMinutos", "EncerramentoForcado", "OrdemVendaSemCliente", "NrCliente"  FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros"';
    let oStatement = oConnection.prepareStatement(sQuery);
    let oResultSet = oStatement.executeQuery();
    
    if(oResultSet.next()) {
        return {
            IdParametro: oResultSet.getInteger(1),
            SLAPadraoMinutos: oResultSet.getDouble(2),
            TempoCarregamentoEmMinutos: oResultSet.getInteger(3),
            EncerramentoForcado: oResultSet.getString(4),
            OrdemVendaSemCliente: oResultSet.getString(5),
            NrCliente: oResultSet.getString(6)
        };
    }
    
    return null;
}

function getPausedProtocols(email) {
    let query = 'SELECT "NumAgrupamento", "MotivoPausa" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    query +=    'WHERE NOT "Excluido" = ? AND "EmailVendedor" = ? AND "Status" = ?';
    
    let stmt = oConnection.prepareStatement(query);
    stmt.setString(1, "X");
    stmt.setString(2, email);
    stmt.setString(3, "Pausada");
    
    let resultSet = stmt.executeQuery();
    let protocols = [];
    
    while(resultSet.next()) {
        protocols.push({
            NumAgrupamento: resultSet.getString(1),
            MotivoPausa: resultSet.getString(2)
        });
    }
    
    return protocols;
}

function checkSLA(protocol, parameters) {
    let sla = new Date(protocol.SLA_OTIF);
    sla.setMinutes(sla.getMinutes() - parameters.TempoCarregamentoEmMinutos);
    
    let diffMs = sla - now;
    let diffMin = (diffMs / 1000) / 60;
    
    if(diffMin > parameters.SLAPadraoMinutos) {
        return true;
    }
    
    return false;
}

function updateProtocol(protocol, parameters, email) {
    let isSlaOk = checkSLA(protocol, parameters, now);
    let query, stmt, updatedRows;
    
    try {
        if(isSlaOk) {
            query = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" SET "Status" = ?, "EmailVendedor" = ?, "AtendimentoInicio" = ?, "KPIValido" = ? ';
            query += 'WHERE "NumAgrupamento" = ?';
            
            stmt = oConnection.prepareStatement(query);
            
            stmt.setString(1, "Em Atendimento");
            stmt.setString(2, email);
            stmt.setTimestamp(3, now);
            stmt.setString(4, "X");
            stmt.setString(5, protocol.NumAgrupamento);
            
            updatedRows = stmt.executeUpdate();
        }
        else {
            query = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" SET "Status" = ?, "EmailVendedor" = ?, "AtendimentoInicio" = ? ';
            query += 'WHERE "NumAgrupamento" = ?;';
            
            stmt = oConnection.prepareStatement(query);
        
            stmt.setString(1, "Em Atendimento");
            stmt.setString(2, email);
            stmt.setTimestamp(3, now);
            stmt.setString(4, protocol.NumAgrupamento);
            
            updatedRows = stmt.executeUpdate();
        }
    } catch(e) {
        return false;
    }
    
    if(updatedRows > 0) {
        return true;
    }
    else {
        return false;
    }
}

function mainFunction() {
    if($.request.method !== $.net.http.GET) {
        $.response.status = $.net.http.METHOD_NOT_ALLOWED;
        $.response.setBody("Only GET method is allowed");
        return;
    }
    
    let parameters = getParameters();
    let email = $.request.parameters.get("Email");
    let pausedProtocols = getPausedProtocols(email);
    let unfinishedProtocol = getUnfinishedProtocol(email);
    
    if(unfinishedProtocol) {
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify({
            Parametros: parameters,
            Pausados: pausedProtocols,
            NumAgrupamento: unfinishedProtocol.NumAgrupamento,
            Data:  unfinishedProtocol.AtendimentoInicio
        }));
        return;
    }
    
    let nextProtocol = getNextProtocol();
    
    if(!nextProtocol) {
        $.response.status = $.net.http.NOT_FOUND;
        $.response.setBody("There is no next protocol in the queue");
        return;
    }
    
    let isUpdateOk = updateProtocol(nextProtocol, parameters, email);
    
    if(!isUpdateOk) {
        $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
        $.response.contentType = 'text/xml';
        $.response.setBody("The next protocol was found, but a error occured while updating it's status");
        return;
    }
    
    oConnection.commit();
    $.response.status = $.net.http.OK;
    $.response.contentType = 'application/json';
    $.response.setBody(JSON.stringify({
        Parametros: parameters,
        Pausados: pausedProtocols,
        NumAgrupamento: nextProtocol.NumAgrupamento,
        Data: now
    }));
    
}

mainFunction();
oConnection.close();