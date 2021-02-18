let oConnection = $.db.getConnection(); 

function getPausedRequest(sEmailVendedor) {
    let sQuery = 'SELECT "NumAgrupamento", "AtendimentoInicio", "PausaEmSegundos" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery +=    'WHERE NOT "Excluido" = ? AND "EmailVendedor" = ? AND "Status" = ?';
    
    let oStatement = oConnection.prepareStatement(sQuery);
    
    oStatement.setString(1, 'X');
    oStatement.setString(2, sEmailVendedor);
    oStatement.setString(3, 'Em Atendimento');
    oStatement.executeQuery();
    
    let oResult = oStatement.getResultSet();
    
    if(oResult.next()) {
        return {
            NumAgrupamento: oResult.getString(1),
            AtendimentoInicio: oResult.getString(2),
            PausaEmSegundos: oResult.getString(3)
        };
    }
    
    return null;
}

function getProtocol () {
    let sQuery = 'SELECT TOP 1 "NumAgrupamento" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery +=    'WHERE NOT "Excluido" = ? AND "Status" = ? ORDER BY "SLA_OTIF" ASC';
    let oStatement = oConnection.prepareStatement(sQuery);
    
    oStatement.setString(1, 'X');
    oStatement.setString(2, 'Aguardando');
    oStatement.executeQuery();
    
    let oResult = oStatement.getResultSet();
    
    if(oResult.next()) {
        return oResult.getString(1);
    }
    
    return null;
}

function updateProtocol(sProtocol, sEmailVendedor, tDate, isSlaOk) {
    try {
        var sQuery, oStatement, iUpdatedRows;
        
        if(isSlaOk) {
            sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" SET "Status" = ?, "EmailVendedor" = ?, "AtendimentoInicio" = ?, "KPIValido" = ? ';
            sQuery += 'WHERE "NumAgrupamento" = ?;';
            
            oStatement = oConnection.prepareStatement(sQuery);
        
            oStatement.setString(1, 'Em Atendimento');
            oStatement.setString(2, sEmailVendedor);
            oStatement.setTimestamp(3, tDate);
            oStatement.setString(4, "X");
            oStatement.setString(5, sProtocol.toString());
            
            iUpdatedRows = oStatement.executeUpdate();
            return iUpdatedRows;
            
        } else {
            sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" SET "Status" = ?, "EmailVendedor" = ?, "AtendimentoInicio" = ? ';
            sQuery += 'WHERE "NumAgrupamento" = ?;';
            
            oStatement = oConnection.prepareStatement(sQuery);
        
            oStatement.setString(1, 'Em Atendimento');
            oStatement.setString(2, sEmailVendedor);
            oStatement.setTimestamp(3, tDate);
            oStatement.setString(4, sProtocol.toString());
            
            iUpdatedRows = oStatement.executeUpdate();
            return iUpdatedRows;
        }
        
    } catch(e) {
        return e.message;
    }
}

function getDateFromString(sDate) {
    var aDate = sDate.split("-");
    return new Date(aDate[0], aDate[1], aDate[2], aDate[3], aDate[4], aDate[5]);
}

function rectifyOTIF(tDate) {
    // O SELECT em uma data no banco, devolve o mês corretamente
    // EX: Novembro = 11. Para criar uma data JS, precisa ajustar o mês para - 1
    return new Date(tDate.setMonth( tDate.getMonth() - 1 ));
}

function getSlaAtendimento() {
    let sQuery = 'SELECT "SLAPadraoMinutos" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros"';
    let oStatement = oConnection.prepareStatement(sQuery);
    
    oStatement.executeQuery();
    
    let oResult = oStatement.getResultSet();
    
    if(oResult.next()) {
        return oResult.getString(1);
    }
}

function getSlaOTIF(sProtocol) {
    let sQuery = 'SELECT "SLA_OTIF" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery += 'WHERE "NumAgrupamento" = ?';
    
    let oStatement = oConnection.prepareStatement(sQuery);
    oStatement.setString(1, sProtocol);
    
    oStatement.executeQuery();
    
    let oResult = oStatement.getResultSet();
    
    if(oResult.next()) {
        let sSla = oResult.getString(1);
        sSla = sSla.substring(0,19);
        sSla = sSla.replace(new RegExp(" ", 'g'), "-");
        sSla = sSla.replace(new RegExp(":", 'g'), "-");
        
        return sSla;
    }
}

function checkSLA(sProtocol, tDate) {
    let sSlaAtendimento = getSlaAtendimento();
    let sSlaOTIF = getSlaOTIF(sProtocol);
    let tSlaOTIF = getDateFromString(sSlaOTIF);
    tSlaOTIF = rectifyOTIF(tSlaOTIF);
    
    let diffMs = (tSlaOTIF - tDate);
    let diffSec = Math.ceil(diffMs / 1000);
    
    let minTime = 60 * sSlaAtendimento; // SLA Atendimento em segundos
    
    if(diffSec > minTime) {
        return true;
    }
    
    return false;
}

function mainFunction() {
    if($.request.method === $.net.http.GET) {
        let sVendedor = $.request.parameters.get("Email");
        // let sDate = $.request.parameters.get("Date");
        let sProtocol = getPausedRequest(sVendedor);
        
        if(sProtocol) {
            $.response.status = $.net.http.OK;
            $.response.contentType = 'application/json';
            $.response.setBody(JSON.stringify(
                {
                    NumAgrupamento: sProtocol.NumAgrupamento,
                    Data: sProtocol.AtendimentoInicio,
                    PausaEmSegundos: sProtocol.PausaEmSegundos,
                    EmailVendedor: sVendedor
                }
            ));
            
            oConnection.commit();
            return;
        } else {
            sProtocol = getProtocol();
        }
        
        if(sProtocol) {
            // let tDateNow = getDateFromString(sDate);
            // let isSlaOk = checkSLA(sProtocol, tDateNow);
            
            // Date GMT-0300
            let tDate = new Date(new Date().setHours(new Date().getHours() - 3));
            let iUpdatedRows = updateProtocol(sProtocol, sVendedor, tDate/*, isSlaOk*/);
            
            if(iUpdatedRows > 0) {
                $.response.status = $.net.http.OK;
                $.response.contentType = 'application/json';
                $.response.setBody(JSON.stringify(
                    {
                        NumAgrupamento: sProtocol,
                        Data: tDate,
                        EmailVendedor: sVendedor
                    }    
                ));
                
                oConnection.commit();
                
            } else {
                $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
                $.response.contentType = 'text/xml';
                $.response.setBody("The update did not work. Error: " + iUpdatedRows);
            }
            
        } else {
            $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
            $.response.contentType = 'text/html';
            $.response.setBody("There is no next request in the queue");
        }
        
    } else {
        // unsupported method
        $.response.status = $.net.http.NOT_IMPLEMENTED;
        $.response.setBody("Only GET method is allowed");
    }
}

mainFunction();
oConnection.close();