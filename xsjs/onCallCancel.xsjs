let conn = $.db.getConnection(); 

function saveLogLigacao(idcl,nmag,nmdc,tmst){
    let sQuery, oStatement;
    sQuery = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_LigacaoFive9" ';
    sQuery += '("Time", "Endpoint", "Vendedor", "NumAgrupamento", "IdCliente", "Disposition", "NumDiscado", "LTimeStamp") ';
    sQuery += 'VALUES(?,?,?,?,?,?,?,?)';
    oStatement = conn.prepareStatement(sQuery);
    oStatement.setTimestamp(1, new Date());
    oStatement.setString(2, "DialingCanceled");
    oStatement.setString(3, '-');
    oStatement.setString(4, idcl);
    oStatement.setString(5, nmag);
    oStatement.setString(6, 'Cancelada');
    oStatement.setString(7, nmdc);
    oStatement.setTimestamp(8, tmst);
    oStatement.executeUpdate();
}
 
function updateTabelaLigacao(nmag,idcl,tmst){
    let sQuery, oStatement, iUpdatedRows;
    sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Ligacoes"';
    sQuery += 'SET "Disposition" = ?, "Resultado" = ? ';
    //sQuery += 'WHERE "NumAgrupamento" = ? and "LigacaoInicio" >= ? and  "LigacaoInicio" <= ? and "IdCliente" = ?';
    sQuery += 'WHERE "NumAgrupamento" = ? and "LigacaoInicio" >= ? and  "LigacaoInicio" <= ?';
    
    oStatement = conn.prepareStatement(sQuery); 
    var tf = new Date(tmst);
    tf = tf.setSeconds(tf.getSeconds() + 1);
    oStatement.setString(1, 'DialingCanceled');
    oStatement.setString(2, 'Ligação Cancelada');
    oStatement.setString(3, nmag);
    oStatement.setTimestamp(4, tmst);
    oStatement.setTimestamp(5, new Date(tf).toJSON().toString());
    //oStatement.setString(5, idcl);
    
    iUpdatedRows = oStatement.executeUpdate();
    return iUpdatedRows;
}

function main() {  
    try {
        let sIDCL = $.request.parameters.get("IDCL");
        let sNMAG = $.request.parameters.get("NMAG");
        let sNMDC = $.request.parameters.get("NMDC");
        let sTMST = new Date(parseInt($.request.parameters.get("TMST"),10));
        var jBody = {
            "VEND":'-',
            "IDCL":sIDCL,
            "NMAG":sNMAG,
            "NMDC":sNMDC,
            "TMST":sTMST
        };
        
        saveLogLigacao(sIDCL,sNMAG,sNMDC,sTMST);
        updateTabelaLigacao(sNMAG,sIDCL,sTMST);
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(jBody));
        
    } catch(e) {
        $.response.status = $.net.http.OK;
        $.response.contentType = "text/html";
        $.response.setBody("body is empty");
    }
    conn.commit();
    conn.close();
}

main();