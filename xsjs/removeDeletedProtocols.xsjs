let connection = $.db.getConnection();
let log = { income: {}, status: "", response: "" };
const now = new Date(new Date().setHours(new Date().getHours() - 3));

// let mock = {
// 	"d": {
// 		"results": [
// 			{
// 				"__metadata": {
// 					"id": "http://brsaolsvfid01.votorantim.grupo:8001/sap/opu/odata/sap/ZGWVCSD_INFO_PRE_AGRUPAMENTO_SRV/ProtocolosExcluidosSet('27099')",
// 					"uri": "http://brsaolsvfid01.votorantim.grupo:8001/sap/opu/odata/sap/ZGWVCSD_INFO_PRE_AGRUPAMENTO_SRV/ProtocolosExcluidosSet('27099')",
// 					"type": "ZGWVCSD_INFO_PRE_AGRUPAMENTO_SRV.ProtocolosExcluidos"
// 				},
// 				"NrComplemento": "27099",
// 				"QtdComplementar": "12.950 ",
// 				"VlrFreteMorto": "376.008 ",
// 				"TimeStamp": "11.06.2020 15:51"
// 			}
// 		]
// 	}
// }

// let mock = {"d":{"results":[]}};

function removeProtocols(protocols) {
    let deletedProtocols = 0;
    
    let query = ` UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"
                  SET "Excluido" = 'X', "AtendimentoFim" = ?, "Status" = 'Excluído pelo SAP', "InterfaceAtuante" = 'Exclusão SAP'
                  WHERE "NumAgrupamento" = ?`;

    try {
        let stmt = connection.prepareStatement(query);
        stmt.setBatchSize(protocols.length);
    
        protocols.forEach(function(protocol) {
            stmt.setTimestamp(1, now);
            stmt.setString(2, protocol.NrComplemento);
            stmt.addBatch();
        });
        
        let records = stmt.executeBatch();
        
        for(let i = 0; i < records.length; i++) {
            if(records[i] !== 1) {
                deletedProtocols = "SQL ERROR: " + records[i];
                break;
            }
            
            deletedProtocols += records[i];
        }
        
        connection.commit();
        
    } catch(e) {
        return e.message;
    }
    
    return deletedProtocols;
}

function deleteLogOlderThanOneMonth() {
    let logConnection = $.db.getConnection();
    let query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.LOG_RemoveDeletedProtocols" WHERE "Time" < ?';
    let lastMonth = new Date(new Date().setDate(new Date().getDate() - 30));

    try {
        let stmt = logConnection.prepareStatement(query);
        stmt.setTimestamp(1, lastMonth);
        stmt.executeUpdate();
        logConnection.commit();
    } catch(e) {
        return;
    }
    
    logConnection.close();
}

function saveLog() {
    deleteLogOlderThanOneMonth();
    
    let query = `INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_RemoveDeletedProtocols"
                 ("Time", "Input", "Status", "Response") VALUES(?, ?, ?, ?)`;

    try {
        let stmt = connection.prepareStatement(query);
        stmt.setTimestamp(1, now);
        stmt.setString(2, log.income ? JSON.stringify(log.income) : null);
        stmt.setString(3, log.status ? log.status.toString() : null);
        stmt.setString(4, log.response ? log.response.toString() : null);
        stmt.executeUpdate();
        connection.commit();
    } catch(e) {
        return;
    }
}

const upsertLog = function (protocols) {
    if (protocols.length === 0) {
        return;
    }
    
    let oConn = $.db.getConnection();
    
    const query = 'UPSERT "COMP_CARGA"."comp_carga.table::cds_table.LOG_CompleCarga" ("NumAgrupamento","Data","Vendedor","Descricao","Informacao") VALUES (?, ?, ?, ?, ?) WITH PRIMARY KEY';
    
    const statement = oConn.prepareStatement(query);
    
    statement.setBatchSize(protocols.length);
    
    protocols.forEach(function (protocol) {
        statement.setString(1, protocol.NrComplemento);
        statement.setString(2, new Date().toISOString());
        statement.setString(3, 'Interface de Exclusão SAP');
        statement.setString(4, 'Pré-Agrupamento excluído pela interface de exclusão do SAP.');
        statement.setString(5, '');
        
        statement.addBatch();
    });
    
    statement.executeBatch();
    oConn.commit();
    oConn.close();
};

function mainFunction() {
    if(!$.request.body) {
        log.status = "error";
        log.response = "No body available";
        $.response.status = $.net.http.BAD_REQUEST;
        $.response.contentType = 'text/html';
        $.response.setBody('No body available');
        return;
    }
    
    // let strBody = JSON.stringify(mock);
    let strBody = $.request.body.asString();
    let jsonBody = JSON.parse(strBody);
    log.income = jsonBody;
    
    if(!jsonBody || !jsonBody.d || !jsonBody.d.results || jsonBody.d.results.length === 0) {
        log.status = "error";
        log.response = "No data to delete";
        $.response.status = $.net.http.BAD_REQUEST;
        $.response.contentType = 'text/html';
        $.response.setBody("No data to delete");
        return; 
    }
    
    let protocols = jsonBody.d.results;
    let deletedProtocols = removeProtocols(protocols);
    
    if(typeof(deletedProtocols) !== "number") {
        log.status = "error";
        log.response = deletedProtocols;
        $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
        $.response.contentType = 'application/json';
        $.response.setBody(JSON.stringify({error: deletedProtocols}));
        return;
    }
    
    upsertLog(protocols);
    
    log.status = "success";
    log.response = deletedProtocols;
    $.response.status = $.net.http.OK;
    $.response.contentType = 'application/json';
    $.response.setBody(JSON.stringify({deletedProtocols: deletedProtocols}));
}

mainFunction();
saveLog();
connection.close();