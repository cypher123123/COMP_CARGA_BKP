let oConn = $.db.getConnection();

function selectStatusCount(sVendedor, sTimeInicio, sTimeFim, sStatus) {
    // let sQuery = 'SELECT COUNT (Protocolo."Status") ';
    // sQuery +=    'FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" AS Protocolo ';
    // sQuery +=    'WHERE "EmailVendedor" = ? AND "AtendimentoFim" BETWEEN ? AND ? AND Protocolo."Status" = ?';
    let sQuery = 'SELECT "Status", "SLA_OTIF", "AtendimentoInicio", "AtendimentoFim", "PausaEmSegundos"';
    sQuery += 'FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" AS Protocolo ';
    sQuery += 'WHERE "EmailVendedor" = ? AND "AtendimentoFim" BETWEEN ? AND ? AND Protocolo."Status" = ?';
    
    let oStmt = oConn.prepareStatement(sQuery); 
    
    oStmt.setString(1, sVendedor.toString());
    oStmt.setTimestamp(2, sTimeInicio);
    oStmt.setTimestamp(3, sTimeFim);
    oStmt.setString(4, sStatus);
    
    let sResultSet = oStmt.executeQuery();
    // let sResponse = "";
    let aResponse = [];
    
    /* eslint-disable no-loop-func */
    while (sResultSet.next()) {
        aResponse.push({
            Status: sResultSet.getNString(1),
            SLA_OTIF: sResultSet.getTimestamp(2),
            AtendimentoInicio: sResultSet.getTimestamp(3),
            AtendimentoFim: sResultSet.getTimestamp(4),
            PausaEmSegundos: sResultSet.getInteger(5)
        });
    }
    
    // while(sResultSet.next()) {
    //     sResponse = sResultSet.getString(1);
    // }
    
    return aResponse;
}

function selectPausadas(sVendedor, sTimeInicio, sTimeFim, sStatus) {
    let sQuery = 'SELECT "Status", "SLA_OTIF", "AtendimentoInicio", "AtendimentoFim", "PausaEmSegundos" ';
    sQuery += 'FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" AS Protocolo ';
    sQuery += 'WHERE "EmailVendedor" = ? AND "TimePausa" BETWEEN ? AND ? AND Protocolo."Status" = ?';
    
    let oStmt = oConn.prepareStatement(sQuery);
    
    oStmt.setString(1, sVendedor.toString());
    oStmt.setTimestamp(2, sTimeInicio);
    oStmt.setString(3, sTimeFim);
    oStmt.setString(4, sStatus);
    
    let sResultSet = oStmt.executeQuery();
    let aResponse = [];
    
    while(sResultSet.next()) {
        aResponse.push({
            Status: sResultSet.getNString(1),
            SLA_OTIF: sResultSet.getTimestamp(2),
            AtendimentoInicio: sResultSet.getTimestamp(3),
            AtendimentoFim: sResultSet.getTimestamp(4),
            PausaEmSegundos: sResultSet.getInteger(5)
        });
    }
    
    return aResponse;
}

function mainFunction() {
    let sVendedor = $.request.parameters.get("Email");
    let sData = $.request.parameters.get("Data");
    
    if(sVendedor && sData) {
        let sTimeInicio = sData + "T00:00:00";
        let sTimeFim = sData + "T23:59:59";
        
        // let oResponse = {};
        let aResponse = [];
        
        aResponse = aResponse.concat(selectStatusCount(sVendedor, sTimeInicio, sTimeFim, "Complemento Total"));
        aResponse = aResponse.concat(selectStatusCount(sVendedor, sTimeInicio, sTimeFim, "Complemento Parcial"));
        aResponse = aResponse.concat(selectStatusCount(sVendedor, sTimeInicio, sTimeFim, "Não Complementado"));
        aResponse = aResponse.concat(selectPausadas(sVendedor, sTimeInicio, sTimeFim, "Pausada"));
        
        // let oResponse = {
        //     ComplementoTotal: selectStatusCount(sVendedor, sTimeInicio, sTimeFim, "Complemento Total"),
        //     ComplementoParcial: selectStatusCount(sVendedor, sTimeInicio, sTimeFim, "Complemento Parcial"),
        //     ComplementoNao: selectStatusCount(sVendedor, sTimeInicio, sTimeFim, "Não Complementado"),
        //     SolicPausadas: selectPausadas(sVendedor, "Pausada")
        // };
        
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(aResponse));
    }
    else {
        $.response.status = $.net.http.OK;
        $.response.contentType = "text/html";
        $.response.setBody("service working");
    }
}

mainFunction();
oConn.close();