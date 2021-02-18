let conn = $.db.getConnection();

function getValueFromXML(tag, xmlString){
     var value, tempString, startTag, endTag, startPos, endPos;
     startTag = "<" + tag + ">";
     endTag = "</" + tag + ">";
     tempString = xmlString;
     startPos = tempString.search(startTag) + startTag.length;
     endPos = tempString.search(endTag);
     value = tempString.slice(startPos,endPos);
     return value;
}
        
// var response = {};
// response.email = getValueFromXML("email", bodyXmlString);
// response.disposition = getValueFromXML("disposition", bodyXmlString

function saveLogIntegracao(content){
    let sQuery, oStatement;
    sQuery = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_IntegracaoFive9" ("Time", "Endpoint", "Body") VALUES(?,?,?)';
    oStatement = conn.prepareStatement(sQuery);
    oStatement.setTimestamp(1, new Date());
    oStatement.setString(2, "onDialingDone");
    oStatement.setString(3, content);
    oStatement.executeUpdate();
}

function saveLogLigacao(vend,idcl,nmag,disp,nmdc,tmst){
    let sQuery, oStatement;
    sQuery = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_LigacaoFive9" ';
    sQuery += '("Time", "Endpoint", "Vendedor", "NumAgrupamento", "IdCliente", "Disposition", "NumDiscado", "LTimeStamp") ';
    sQuery += 'VALUES(?,?,?,?,?,?,?,?)';
    oStatement = conn.prepareStatement(sQuery);
    oStatement.setTimestamp(1, new Date());
    oStatement.setString(2, "onDialingDone");
    oStatement.setString(3, vend);
    oStatement.setString(4, idcl);
    oStatement.setString(5, nmag);
    oStatement.setString(6, disp);
    oStatement.setString(7, nmdc);
    oStatement.setString(8, tmst);
    oStatement.executeUpdate();
}

function updateTabelaLigacao(nmag,idcl,tmst,disp){
    let sQuery, oStatement, iUpdatedRows;
    sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Ligacoes"';
    sQuery += 'SET "Disposition" = ?';
    sQuery += 'WHERE "NumAgrupamento" = ? and "LigacaoInicio" >= ? and  "LigacaoInicio" <= ? and "IdCliente" = ?';
    
    oStatement = conn.prepareStatement(sQuery); 
    var tf = new Date(tmst);
    tf = tf.setSeconds(tf.getSeconds() + 1);
    oStatement.setString(1, disp);
    oStatement.setString(2, nmag);
    oStatement.setTimestamp(3, tmst);
    oStatement.setTimestamp(4, new Date(tf).toJSON().toString());
    oStatement.setString(5, idcl);
    
    iUpdatedRows = oStatement.executeUpdate();
    return iUpdatedRows;
}

function main() {  
    try {
        let sVEND = $.request.parameters.get("VEND");
        let sIDCL = $.request.parameters.get("IDCL");
        let sNMAG = $.request.parameters.get("NMAG");
        let sNMDC = $.request.parameters.get("NMDC");
        let sTMST = $.request.parameters.get("TMST");
        let sDISP = $.request.parameters.get("DISP");
        var jBody = {
            "VEND":sVEND,
            "IDCL":sIDCL,
            "NMAG":sNMAG,
            "NMDC":sNMDC,
            "TMST":sTMST,
            "DISP":sDISP
        };
        
        saveLogIntegracao(JSON.stringify(jBody));
        saveLogLigacao(sVEND,sIDCL,sNMAG,sDISP,sNMDC,sTMST);
        updateTabelaLigacao(sNMAG,sIDCL,sTMST,sDISP);
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(jBody));
        
    } catch(e) {
        saveLogIntegracao(e.message);
        $.response.status = $.net.http.OK;
        $.response.contentType = "text/html";
        $.response.setBody("body is empty");
    }
    
    
    conn.commit();
    conn.close();
}

main();