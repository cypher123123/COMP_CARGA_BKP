let oConn = $.db.getConnection();

function getStartDate() {
    // -3h para deixar o GMT igual o do Brasil
    let now = new Date(new Date().setHours(new Date().getHours() - 3));
    
    // +2 dias úteis
    let startDate = new Date(now.setDate(now.getDate() + 2));
    
    // Se for sábado, acrescenta 2 dias pra retornar segunda-feira
    if(startDate.getDay() === 6) {
        return new Date(startDate.setDate(startDate.getDate() + 2));
    }
    
    // Se for domingo, acrescenta 1 dia pra retornar segunda-feira
    if(startDate.getDay() === 0) {
        return new Date(startDate.setDate(startDate.getDate() + 1));
    }
    
    return startDate;
}

function getEndDate(startDate) {
    // +1 dia no startDate
    let startTime = startDate.getTime();
    let endDate = new Date(new Date(startTime).setDate(startDate.getDate() + 1));
    
    // Se for sábado, acrescenta 2 dias pra retornar segunda-feira
    if(endDate.getDay() === 6) {
        return new Date(endDate.setDate(endDate.getDate() + 2));
    }
    
    // Se for domingo, acrescenta 1 dia pra retornar segunda-feira
    if(endDate.getDay() === 0) {
        return new Date(endDate.setDate(endDate.getDate() + 1));
    }
    
    return endDate;
}

function getNotFullyFilledProtocols() {
    let query = `SELECT "NumAgrupamento", "Status" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"
                 WHERE ("SLA_OTIF" BETWEEN ? AND ?) AND
                       ("Status" = 'Complemento Parcial' OR "Status" = 'Não Complementado') AND
                       NOT ("Excluido" = 'X')`;
    
    const oStatement = oConn.prepareStatement(query);
    
    let startDate = getStartDate();
    let endDate = getEndDate(startDate);

	oStatement.setTimestamp(1, startDate);
	oStatement.setTimestamp(2, endDate);

	const oResult = oStatement.executeQuery();
	let aProtocols = [];

	while (oResult.next()) {
		aProtocols.push({
			NumAgrupamento: oResult.getString(1),
			Status: oResult.getNString(2)
		});
	}

	return aProtocols;
}

function getProtocolsSalesOrder (aProtocols) {
    let sQuery =  'SELECT "NumAgrupamento"';
    sQuery += 'FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" ';
    sQuery += 'WHERE "NumAgrupamento" = ? AND NOT "OrdemDoComplemento" = 1';
    
    let aSalesOrder = [];
    aProtocols.forEach(function (oProtocol) {
        let oStmt = oConn.prepareStatement(sQuery);
        oStmt.setString(1, oProtocol.NumAgrupamento.toString());
        
        let oResultSet = oStmt.executeQuery();
        
        
        while (oResultSet.next()) {
            aSalesOrder.push(oResultSet.getString(1));
        }
    });
    
    return aSalesOrder;
}

function getResponseData (aProtocols) {
    let aSalesOrder = getProtocolsSalesOrder(aProtocols);
    
    return {
        Dummy: '',
        'ProtocolosVencidosSet': aProtocols.map(function (oProtocol) {
            let aProtocolOrders = aSalesOrder.filter(function (order) {
                return order.toString() === oProtocol.NumAgrupamento.toString();
            });
            
            return {
                NrComplemento: oProtocol.NumAgrupamento.toString(),
                Status: oProtocol.Status,
                houveOrdem: aProtocolOrders.length ? 'X' : '',
                Excluido: 'X'
            };
        })
    };
}

function deleteLogOlderThanOneMonth() {
    let logConnection = $.db.getConnection();
    let query = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.LOG_GetNotFullyFilledProtocols" WHERE "Time" < ?';
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

function saveLog(data) {
    deleteLogOlderThanOneMonth();
    
    let logConnection = $.db.getConnection();
    let query = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_GetNotFullyFilledProtocols" ("Time", "Input") VALUES(?, ?)';
    const now = new Date();

    try {
        let stmt = logConnection.prepareStatement(query);
        stmt.setTimestamp(1, now);
        stmt.setString(2, data ? JSON.stringify(data) : null);
        stmt.executeUpdate();
        logConnection.commit();
    } catch(e) {
        return;
    }
    
    logConnection.close();
}

function mainFunction() {
    try {
        const aProtocols = getNotFullyFilledProtocols();
        const response = getResponseData(aProtocols);
        saveLog(response);
        
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(response));
    
    } catch(e) {
        $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
        $.response.contentType = "text/html";
        $.response.setBody( e.message );
    }
    
}

mainFunction();
oConn.close();