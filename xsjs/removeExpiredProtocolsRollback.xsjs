const oConn = $.db.getConnection();

const upsertLog = function (protocols) {
    if (protocols.length === 0) {
        return;
    }
    
    const query = 'UPSERT "COMP_CARGA"."comp_carga.table::cds_table.LOG_CompleCarga" ("NumAgrupamento","Data","Vendedor","Descricao","Informacao") VALUES (?, ?, ?, ?, ?) WITH PRIMARY KEY';
    
    const statement = oConn.prepareStatement(query);
    
    statement.setBatchSize(protocols.length);
    
    protocols.forEach(function (protocol) {
        statement.setString(1, protocol.NumAgrupamento);
        statement.setString(2, new Date().toISOString());
        statement.setString(3, '');
        statement.setString(4, 'Pré-agrupamento restaurado pelo rollback da interface de encerramento.');
        statement.setString(5, '');
        
        statement.addBatch();
    });
    
    statement.executeBatch();
    oConn.commit();
};

const oFunctions = {
    mainFunction: function () {
        return {
            iProtocolsRollback: this.rollbackProtocols()
        };
    },
    
    getRollbackProtocols: function () {
        const sQuery = 'SELECT "NumAgrupamento", "Status", "Excluido", "InterfaceAtuante" FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloRollback"';
        const oStatement = oConn.prepareStatement(sQuery);
    	const oResultSet = oStatement.executeQuery();
    	const aResult = [];
    	
    	while (oResultSet.next()) {
            aResult.push({
                NumAgrupamento: oResultSet.getString(1),
                Status: oResultSet.getString(2),
                Excluido: oResultSet.getString(3),
                InterfaceAtuante: oResultSet.getString(4)
            });
    	}
    	
    	return aResult;
    },
    
    rollbackProtocols: function () {
        let aProtocols = this.getRollbackProtocols();
        
        if (aProtocols.length === 0) {
            return 0;
        }
        
        let sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
        sQuery += 'SET "Status" = ?, "AtendimentoFim" = NULL, "Excluido" = ?, "InterfaceAtuante" = ? ';
        sQuery += 'WHERE "NumAgrupamento" = ?';
        
        const oStatement = oConn.prepareStatement(sQuery);
        oStatement.setBatchSize(aProtocols.length);
        
        aProtocols.forEach(function(oProtocol) {
            oStatement.setString(1, oProtocol.Status);
            oStatement.setString(2, oProtocol.Excluido);
            oStatement.setString(3, oProtocol.InterfaceAtuante);
            oStatement.setString(4, oProtocol.NumAgrupamento);
            
            oStatement.addBatch();
        });
        
        const iProtocolsRollback = oStatement.executeBatch();
        oConn.commit();
        
        upsertLog(aProtocols);
        
        if (iProtocolsRollback) {
            return iProtocolsRollback.length;
        }
        
        return 0;
    },
    
    deleteRollbackLog: function () {
        let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloRollback"';
        
        const oStmt = oConn.prepareStatement(sQuery);
        const oResultSet = oStmt.executeQuery();
        
        let iDeletedRollbackProtocols = 0;
        
        while (oResultSet.next()) {
            iDeletedRollbackProtocols++;
        }
        
        oConn.commit();
        
        return iDeletedRollbackProtocols;
    },
    
    sendResponse: function (response) {
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(response));
    },
    
    sendError: function (error) {
        $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
        $.response.contentType = "text/html";
        $.response.setBody(JSON.stringify(error));
    }
};

try {
    oFunctions.sendResponse(oFunctions.mainFunction());
    oFunctions.deleteRollbackLog();
} catch (err) {
    oFunctions.sendError(err);
}