let oConn = $.db.getConnection();

function selectVolumeVenda(sTimeInicio, sTimeFim) {
    let sQuery = 'SELECT TO_INTEGER(SUM(ov."ToneladasOV")) as QuantidadeVenda,vd."NomeVendedor" ';
    sQuery += ' FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" ov';
    sQuery += ' FULL JOIN "COMP_CARGA"."comp_carga.table::cds_table.Vendedor" vd';
    sQuery += ' ON vd."EmailVendedor" = ov."EmailVendedor" ';
    sQuery += ' WHERE vd."NomeVendedor" is not null and ov."DataOrdemVenda" between ? and ? ';
    sQuery += ' GROUP BY vd."NomeVendedor"';
    sQuery += ' ORDER BY QuantidadeVenda desc';
    let oStmt = oConn.prepareStatement(sQuery);
     
    oStmt.setTimestamp(1, sTimeInicio); 
    oStmt.setTimestamp(2, sTimeFim);
    
    let sResultSet = oStmt.executeQuery();
    let aResponse = [];
    
    while(sResultSet.next()) {
        aResponse.push({
            Vendedor: sResultSet.getNString(2),
            QuantidadeVenda: sResultSet.getNString(1)
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
        
        aResponse = aResponse.concat(selectVolumeVenda(sTimeInicio, sTimeFim));
        
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