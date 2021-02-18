let oConnection = $.db.getConnection();

function getParameters() {
    let sQuery = 'SELECT "IdParametro", "SLAPadraoMinutos", "TempoCarregamentoEmMinutos", "EncerramentoForcado", "OrdemVendaSemCliente", "NrCliente" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros"';
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

function getPausa(protocol) {
    let query = 'SELECT "TimePausa", "PausaEmSegundos", "AtendimentoInicio" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    query +=    'WHERE "NumAgrupamento" = ?';
    
    let stmt = oConnection.prepareStatement(query);
    stmt.setString(1, protocol);
    
    let resultSet = stmt.executeQuery();
    
    if(resultSet.next()) {
        return {
            TimePausa: resultSet.getTimestamp(1),
            PausaEmSegundos: resultSet.getInteger(2),
            AtendimentoInicio: resultSet.getTimestamp(3)
        };
    }
    
    return null;
}

function getDiffInSeconds(now, timePausa) {
    let diffMs = now - timePausa;
    return diffMs / 1000;
}

function checkSLA(protocol, parameters, now) {
    let sla = new Date(protocol.SLA_OTIF);
    sla.setMinutes(sla.getMinutes() - parameters.TempoCarregamentoEmMinutos);
    
    let diffMs = sla - now;
    let diffMin = (diffMs / 1000) / 60;
    
    if(diffMin > parameters.SLAPadraoMinutos) {
        return true;
    }
    
    return false;
}

function updateProtocol(protocol) {
    // now tem o valor do gmt brasilia
    let now = new Date(new Date().setHours(new Date().getHours() - 3));
    let pausa = getPausa(protocol);
    let pausaEmSegundos = 0;
    
    if(pausa.TimePausa) {
        pausaEmSegundos = getDiffInSeconds(now, pausa.TimePausa) - 60;
    }
    
    if(pausa.PausaEmSegundos) {
        pausaEmSegundos += pausa.PausaEmSegundos;
    }
    
    let query, stmt;
    let updatedRows = 0;
    
    try {
        if (pausa.AtendimentoInicio === null) {
            let isSlaOk = checkSLA(protocol, getParameters(), now);
            let isValidKPI = "";
        
            if (isSlaOk) {
                isValidKPI = "X";
            }
            
            query = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" SET "Status" = ?, "PausaEmSegundos" = ?, "MotivoPausa" = ?, "MotivoObservacao" = ?, ' + 
            '"AtendimentoInicio" = ?, "KPIValido" = ? ';
            query += 'WHERE "NumAgrupamento" = ?';
            
            stmt = oConnection.prepareStatement(query);
            
            stmt.setString(1, "Em Atendimento");
            stmt.setInteger(2, pausaEmSegundos);
            stmt.setString(3, "");
            stmt.setString(4, "");
            stmt.setTimestamp(5, now);
            stmt.setString(6, isValidKPI);
            stmt.setString(7, protocol);
            
            updatedRows = stmt.executeUpdate();
            
        } else {
            query = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" SET "Status" = ?, "PausaEmSegundos" = ?, "MotivoPausa" = ?, "MotivoObservacao" = ? ';
            query += 'WHERE "NumAgrupamento" = ?';
            
            stmt = oConnection.prepareStatement(query);
            
            stmt.setString(1, "Em Atendimento");
            stmt.setInteger(2, pausaEmSegundos);
            stmt.setString(3, "");
            stmt.setString(4, "");
            stmt.setString(5, protocol);
            
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
    
    let protocol = $.request.parameters.get("Protocolo");
    let email = $.request.parameters.get("Email");
    
    let parameters = getParameters();
    
    let isUpdateOk = updateProtocol(protocol);
    
    if(!isUpdateOk) {
        $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
        $.response.contentType = 'text/xml';
        $.response.setBody("The next protocol was found, but a error occured while updating it's status");
        return;
    }
    oConnection.commit();
    
    let pausedProtocols = getPausedProtocols(email);
    
    $.response.status = $.net.http.OK;
    $.response.contentType = 'application/json';
    $.response.setBody(JSON.stringify({
        success: true,
        Parametros: parameters,
        Pausados: pausedProtocols
    }));
}

mainFunction();
oConnection.close();