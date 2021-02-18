/* eslint-disable max-len */
let json = {
	"d": {
		"results": [
			{
				"NrComplemento": "5326"
			},
			{
				"NrComplemento": "5328"
			},
			{
				"NrComplemento": "3333"
			}
		]
	}
};

function sendResponse(obj) {
    if(typeof obj === "object") {
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(obj));
    } else {
        $.response.status = $.net.http.OK;
        $.response.contentType = "text/html";
        $.response.setBody(obj);
    }
}

function deleteProtocolos(aProtocols) {
    if(aProtocols.length === 0) {
        return 0;
    }
    
    let sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery += 'SET "Excluido" = ?';
    sQuery += 'WHERE "NumAgrupamento" = ?';
    // let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" WHERE "NumAgrupamento" = ?;';
    
    try {
        let oConn = $.db.getConnection();
        let oStmt = oConn.prepareStatement(sQuery);
        oStmt.setBatchSize(aProtocols.length);
        
        aProtocols.forEach(function(oProtocol) {
            oStmt.setString(1, "X");
            oStmt.setString(2, oProtocol.NrComplemento.toString());
        	oStmt.addBatch();
    	});
        
    	let aRecords = oStmt.executeBatch();
        oConn.commit();
        
        let iDeletedProtocols = 0;
        
        aRecords.forEach(function(iRecord) {
            iDeletedProtocols += iRecord;
        });
    	
    	oConn.close();
    	return iDeletedProtocols;
    	
    } catch(e) {
        oConn.close();
        return e.message;
    }
}

function deleteProtocoloCliente(aProtocols) {
    if(aProtocols.length === 0) {
        return 0;
    }
    
    let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloCliente" WHERE "NumAgrupamento" = ?;';
    
    try {
        let oConn = $.db.getConnection();
        let oStmt = oConn.prepareStatement(sQuery);
        oStmt.setBatchSize(aProtocols.length);
        
        aProtocols.forEach(function(oProtocol) {
            oStmt.setString(1, oProtocol.NrComplemento.toString());
        	oStmt.addBatch();
    	});
        
    	let aRecords = oStmt.executeBatch();
        oConn.commit();
        
        let iDeletedProtocols = 0;
        
        aRecords.forEach(function(iRecord) {
            iDeletedProtocols += iRecord;
        });
    	
    	oConn.close();
    	return iDeletedProtocols;
    	
    } catch(e) {
        oConn.close();
        return e.message;
    }
}

function deleteUltimasComprasHeader(aProtocols) {
    if(aProtocols.length === 0) {
        return 0;
    }
    
    let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.UltimasComprasHeader" WHERE "NumAgrupamento" = ?;';
    
    try {
        let oConn = $.db.getConnection();
        let oStmt = oConn.prepareStatement(sQuery);
        oStmt.setBatchSize(aProtocols.length);
        
        aProtocols.forEach(function(oProtocol) {
            oStmt.setString(1, oProtocol.NrComplemento.toString());
        	oStmt.addBatch();
    	});
        
    	let aRecords = oStmt.executeBatch();
        oConn.commit();
        
        let iDeletedProtocols = 0;
        
        aRecords.forEach(function(iRecord) {
            iDeletedProtocols += iRecord;
        });
    	
    	oConn.close();
    	return iDeletedProtocols;
    	
    } catch(e) {
        oConn.close();
        return e.message;
    }
}

function deleteUltimasComprasItens(aProtocols) {
    if(aProtocols.length === 0) {
        return 0;
    }
    
    let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.UltimasComprasItens" WHERE "NumAgrupamento" = ?;';
    
    try {
        let oConn = $.db.getConnection();
        let oStmt = oConn.prepareStatement(sQuery);
        oStmt.setBatchSize(aProtocols.length);
        
        aProtocols.forEach(function(oProtocol) {
            oStmt.setString(1, oProtocol.NrComplemento.toString());
        	oStmt.addBatch();
    	});
        
    	let aRecords = oStmt.executeBatch();
        oConn.commit();
        
        let iDeletedProtocols = 0;
        
        aRecords.forEach(function(iRecord) {
            iDeletedProtocols += iRecord;
        });
    	
    	oConn.close();
    	return iDeletedProtocols;
    	
    } catch(e) {
        oConn.close();
        return e.message;
    }
}

function mainFunction() {
    let sJsonBody = $.request.body.asString();
    let oJsonBody;
    let aResults;
    
    try {
        oJsonBody = JSON.parse(sJsonBody);
        aResults = oJsonBody.d.results;
        // aResults = json.d.results;
        
        sendResponse(aResults);
        
    } catch(e) {
        $.response.status = $.net.http.BAD_REQUEST;
        $.response.contentType = "text/html";
        $.response.setBody("Error: The input object is not in the expected format");
        return;
    }
    
    try {
        let iProtocolo = deleteProtocolos(aResults);
        // let iProtocoloCliente = deleteProtocoloCliente(aResults);
        // let iUltimasComprasHeader = deleteUltimasComprasHeader(aResults);
        // let iUltimasComprasItens = deleteUltimasComprasItens(aResults);
        
        sendResponse({
            "DeletedProtocolo": iProtocolo
            // "DeletetedProtocoloCliente": iProtocoloCliente,
            // "DeletetedUltimasComprasHeader": iUltimasComprasHeader,
            // "DeletetedUltimasComprasItens": iUltimasComprasItens
        });
        
    } catch(e) {
        $.response.status = $.net.http.BAD_REQUEST;
        $.response.contentType = "text/html";
        $.response.setBody(e.message);
        return;
    }
}

mainFunction();
