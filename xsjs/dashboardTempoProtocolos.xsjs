let oConn = $.db.getConnection();

function selectTempoProtocolo(sTimeInicio, sTimeFim) {
    let sQuery = 'select "NumAgrupamento" , "Status", (select IFNULL(TO_INTEGER(AVG(SECONDS_BETWEEN(';
    sQuery += '"COMP_CARGA"."comp_carga.table::cds_table.Ligacoes"."LigacaoInicio","COMP_CARGA"."comp_carga.table::cds_table.Ligacoes"."LigacaoFim"))),0)';
    sQuery += ' from "COMP_CARGA"."comp_carga.table::cds_table.Ligacoes" ';
    sQuery += ' where "COMP_CARGA"."comp_carga.table::cds_table.Ligacoes"."NumAgrupamento" = "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"."NumAgrupamento" ';
    sQuery += ' and "COMP_CARGA"."comp_carga.table::cds_table.Ligacoes"."LigacaoInicio" between ? and ? ) as tempo_atendimento ';
    sQuery += ' from "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"';
    sQuery += ' where "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"."AtendimentoInicio" between ? and ? or';
    sQuery += ' "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"."AtendimentoFim" between ? and ?';
    sQuery += ' order by tempo_atendimento desc';
    let oStmt = oConn.prepareStatement(sQuery);
     
    oStmt.setTimestamp(1, sTimeInicio);
    oStmt.setTimestamp(2, sTimeFim);
    oStmt.setTimestamp(3, sTimeInicio);
    oStmt.setTimestamp(4, sTimeFim);
    oStmt.setTimestamp(5, sTimeInicio);
    oStmt.setTimestamp(6, sTimeFim);
    
    let sResultSet = oStmt.executeQuery();
    let aResponse = [];
    
    while(sResultSet.next()) {
        aResponse.push({
            NumAgrupamento: sResultSet.getNString(1),
            Status: sResultSet.getNString(2),
            Tempo: sResultSet.getNString(3)
        });
    }

    return aResponse;
}

function mainFunction() {
    let sDataIni = $.request.parameters.get("DataIni");
    let sDataFim = $.request.parameters.get("DataFim");
    if(sDataIni && sDataFim) {
        let sTimeInicio = sDataIni + "T00:00:00";
        let sTimeFim = sDataFim + "T23:59:59";
        
        // let oResponse = {};
        let aResponse = [];
        
        aResponse = aResponse.concat(selectTempoProtocolo(sTimeInicio, sTimeFim));
        
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