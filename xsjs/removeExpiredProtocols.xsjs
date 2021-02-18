/* eslint-disable max-len */
let oConn = $.db.getConnection();

function getParametros() {
    let sQuery = 'SELECT "TempoCarregamentoEmMinutos", "InicioCelula", "FimCelula" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros"';
    let oStmt = oConn.prepareStatement(sQuery);
    let oResultSet = oStmt.executeQuery();
    
    while (oResultSet.next()) {
        return {
            TMAC: oResultSet.getInteger(1),
            InicioCelula: oResultSet.getNString(2),
            FimCelula: oResultSet.getNString(3)
        };
    }
    
    return {};
}

function deleteRollbackLog () {
    let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloRollback"';
    
    const oStmt = oConn.prepareStatement(sQuery);
    
    const oResultSet = oStmt.executeQuery();
    
    let iDeletedRollbackProtocols = 0; 
    
    while (oResultSet.next()) {
        iDeletedRollbackProtocols++;
    }
    
    oConn.commit();
    
    return iDeletedRollbackProtocols;
}

function getStatus (oProtocol) {
    const ToneladasRestantes = parseFloat(oProtocol.ToneladasRestantes);
    const ToneladasRestantesInicial = parseFloat(oProtocol.ToneladasRestantesInicial);
    const MinimaToneladasRestante = parseFloat(oProtocol.MinimaToneladasRestante);
    
    if (isNaN(ToneladasRestantes)) {
        return '';
    } else if (ToneladasRestantes === ToneladasRestantesInicial) {
        return 'Não Complementado';
    } else if (ToneladasRestantes <= MinimaToneladasRestante) {
        return 'Complemento Total';
    } else {
        return 'Complemento Parcial';
    }
}

function getExpiredProtocols(Parametros) {
    let sQuery = 'SELECT "NumAgrupamento", "Status", "SLA_OTIF", "ToneladasRestantes", "ToneladasRestantesInicial", "MinimaToneladasRestante", "Excluido", (SELECT "HoraLimiteInterface" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros") AS "HoraLimiteInterface" ';
    sQuery +=    'FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery +=    'WHERE ("Status" = \'Pausada\' OR "Status" = \'Aguardando\')';
    
    let oStmt = oConn.prepareStatement(sQuery);
    
    let oResultSet = oStmt.executeQuery();
    
    let aProtocols = [];
    let iCarregamento = Parametros.TMAC / 60;
    const now = new Date(new Date().setHours(new Date().getHours() - 3));
    const ISODate = now.toISOString().split('T')[0];
    const CelulaInicio = new Date(ISODate + 'T' + Parametros.InicioCelula + ':00'); 
    const CelulaFim = new Date(ISODate + 'T' + Parametros.FimCelula + ':00'); 
    
    while (oResultSet.next()) {
        let date = new Date(oResultSet.getTimestamp(3));
        let sla = new Date(date.setHours(date.getHours() - iCarregamento));
        
        if (sla < now) {
            aProtocols.push({
                NumAgrupamento: oResultSet.getString(1),
                Status: oResultSet.getString(2),
                ToneladasRestantes: oResultSet.getString(4),
                ToneladasRestantesInicial: oResultSet.getString(5),
                MinimaToneladasRestante: oResultSet.getString(6),
                Excluido: oResultSet.getString(7)
            });
        } else if(sla > CelulaFim) {
            let HoraLimiteInterface = new Date(new Date().setHours(now.getHours() + oResultSet.getInteger(8)));
            
            if (sla < HoraLimiteInterface) {
                aProtocols.push({
                    NumAgrupamento: oResultSet.getString(1),
                    Status: oResultSet.getString(2),
                    ToneladasRestantes: oResultSet.getString(4),
                    ToneladasRestantesInicial: oResultSet.getString(5),
                    MinimaToneladasRestante: oResultSet.getString(6),
                    Excluido: oResultSet.getString(7)
                });
            }
        }
    }
    
    return aProtocols;
}

function setVendor (protocols) {
    if (protocols.length === 0) {
        return;
    }
    
    let query = `UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" 
                 SET "InterfaceAtuante" = 'Encerramento'
                 WHERE "NumAgrupamento" = ?`;
    
    const statement = oConn.prepareStatement(query);
    
    statement.setBatchSize(protocols.length);
    protocols.forEach(function (protocol) {
        statement.setString(1, protocol);
        
        statement.addBatch();
    });
    
    statement.executeBatch();
    oConn.commit();
}

function updateExpiredProtocols(aProtocols) {
    if (aProtocols.length === 0) {
        return;
    }
    
    let sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery += 'SET "Status" = ?, "AtendimentoFim" = ? ';
    sQuery += 'WHERE "NumAgrupamento" = ?';
    
    const oStmt = oConn.prepareStatement(sQuery);
    oStmt.setBatchSize(aProtocols.length);
    const now = new Date(new Date().setHours(new Date().getHours() - 3));
    
    aProtocols.forEach(function(oProtocol) {
        oStmt.setString(1, getStatus(oProtocol));
        oStmt.setTimestamp(2, now);
        oStmt.setString(3, oProtocol.NumAgrupamento);
        
        oStmt.addBatch();
    });
    
    oStmt.executeBatch();
    oConn.commit();
    setVendor(aProtocols.map(function (protocol) { return protocol.NumAgrupamento; }));
    
    // aProtocols.forEach(function(oProtocol) {
    //     let sQuery = 'SELECT COUNT("NumAgrupamento") FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" WHERE "NumAgrupamento" = ?';
    //     let oStmt = oConn.prepareStatement(sQuery);
    //     oStmt.setString(1, oProtocol.NumAgrupamento);
        
    //     let oResultSet = oStmt.executeQuery();
    //     let count = 0;
        
    //     while (oResultSet.next()) {
    //         count = oResultSet.getInteger(1);
    //     }
        
    //     if(count > 0) {
    //         let sQuery2 = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    //         sQuery2 += 'SET "Status" = ? WHERE "NumAgrupamento" = ?';
            
    //         let oStmt2 = oConn.prepareStatement(sQuery2);
    //         oStmt2.setString(1, "Complemento Parcial");
    //         oStmt2.setString(2, oProtocol.NumAgrupamento);
        
    //     } else {
    //         let sQuery3 = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    //         sQuery3 += 'SET "Status" = ? WHERE "NumAgrupamento" = ?';
            
    //         let oStmt3 = oConn.prepareStatement(sQuery3);
    //         oStmt3.setString(1, "Não Complementado");
    //         oStmt3.setString(2, oProtocol.NumAgrupamento);
            
    //     }
            
    //     oConn.commit();
        
    // });
    
    
    // let sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    // sQuery += 'SET "Excluido" = ? ';
    // sQuery += 'WHERE "NumAgrupamento" = ? ';
    
    // let oStmt = oConn.prepareStatement(sQuery);
    // oStmt.setBatchSize(aProtocols.length);
    
    // aProtocols.forEach(function(sProtocol) {
    //     oStmt.setString(1, "X");
    //     oStmt.setString(2, sProtocol);
        
    //     oStmt.addBatch();
    // });
    
    // let iRows = 0;
    // let aRecords = oStmt.executeBatch();
    // oConn.commit();
    
    // aRecords.forEach(function(iRecord) {
    //     iRows += iRecord;
    // });
    
    // return iRows;
}

function deleteExpiredProtocols(aProtocols) {
    let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" WHERE "NumAgrupamento" = ?';
    let oStmt = oConn.prepareStatement(sQuery);
    oStmt.setBatchSize(aProtocols.length);
    
    aProtocols.forEach(function(sProtocol) {
        oStmt.setString(1, sProtocol);
        oStmt.addBatch();
    });
    
    let iRows = 0;
    let aRecords = oStmt.executeBatch();
    oConn.commit();
    
    aRecords.forEach(function(iRecord) {
        iRows += iRecord;
    });
    
    return iRows;
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
                Status: getStatus(oProtocol),
                houveOrdem: aProtocolOrders.length ? 'X' : '',
                Excluido: oProtocol.Delete ? 'X' : ''
            };
        })
    };
}

function getProtocolsToDelete() {
	let query = 'SELECT "NumAgrupamento", "Status", "Excluido", "InterfaceAtuante" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
	query += 'WHERE ("SLA_OTIF" BETWEEN ? AND ?)';
	query += 'AND ("Status" = \'Complemento Parcial\' OR "Status" = \'Não Complementado\') ';
	query += 'AND NOT ("Excluido" = \'X\')';

	const oStatement = oConn.prepareStatement(query);

    let start = new Date(new Date().setHours(new Date().getHours() - 3));
    start.setDate(start.getDate() + 1);
    let end = new Date(new Date(start).setDate(start.getDate() + 2));

	oStatement.setTimestamp(1, start);
	oStatement.setTimestamp(2, end);

	const oResult = oStatement.executeQuery();
	let aProtocols = [];

	while (oResult.next()) {
		aProtocols.push({
			NumAgrupamento: oResult.getString(1),
			Status: oResult.getString(2),
			Excluido: oResult.getString(3),
			InterfaceAtuante: oResult.getString(4)
		});
	}

	return aProtocols;
}

function removeProtocols(aProtocols) {
    if(aProtocols.length === 0) {
        return;
    }
    
    let sQuery = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    sQuery += 'SET "Excluido" = \'X\'';
    sQuery += 'WHERE "NumAgrupamento" = ?';
    // let sQuery = 'DELETE FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" WHERE "NumAgrupamento" = ?;';
    
    try {
        let oStmt = oConn.prepareStatement(sQuery);
        oStmt.setBatchSize(aProtocols.length);
        
        aProtocols.forEach(function(oProtocol) {
            oStmt.setString(1, oProtocol.NumAgrupamento.toString());
        	oStmt.addBatch();
    	});
        
    	let aRecords = oStmt.executeBatch();
        oConn.commit();
        
        let iDeletedProtocols = 0;
        
        aRecords.forEach(function(iRecord) {
            iDeletedProtocols += iRecord;
        });
    	
    } catch(e) {
        oConn.close();
    }
}

function deleteProtocols () {
    let aProtocolsDelete = getProtocolsToDelete();
    removeProtocols(aProtocolsDelete);
    
    return aProtocolsDelete;
}

function removeDuplicates(arr1, arr2) {
	let sendData = [];

	if (!arr1.length) {
		return arr2;
	}

	for (let i = 0; i < arr1.length; i++) {
		let oProtocol = arr1[i];
		let oDeletedProtocol;
		let contains = false;

		for (let j = 0; j < arr2.length; j++) {
			oDeletedProtocol = arr2[j];

			if (oProtocol.NumAgrupamento === oDeletedProtocol.NumAgrupamento) {
				// contains = true;
				oDeletedProtocol.contains = true;
				contains = true;
				break;
			} else {
			    oDeletedProtocol.contains = false;
			}
		}

		if (!contains) {
			sendData.push(oProtocol);
			contains = false;
		}
	}

	return sendData.concat(
	    arr2.filter(function (oDeletedProtocol) {
	        return !oDeletedProtocol.contains;
	    })
	);
}

function insertProtocolRollbackLog (aProtocols) {
    if (aProtocols.length === 0) {
        return 0;
    }
    
    let sQuery = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloRollback" ("NumAgrupamento", "Status", "Excluido", "InterfaceAtuante") ';
    sQuery += 'VALUES (?, ?, ?, ?)';
    
    const oStatement = oConn.prepareStatement(sQuery);
    oStatement.setBatchSize(aProtocols.length);
    
    aProtocols.forEach(function (oProtocol) {
        oStatement.setString(1, oProtocol.NumAgrupamento);
        oStatement.setString(2, oProtocol.Status);
        oStatement.setString(3, oProtocol.Excluido);
        oStatement.setString(4, oProtocol.InterfaceAtuante);
        
        oStatement.addBatch();
    });
    
    const iInsertedRollbackProtocols = oStatement.executeBatch();
    oConn.commit();
    
    if (iInsertedRollbackProtocols) {
        return iInsertedRollbackProtocols.length;
    }
    
    return 0;
}

function getRemoveExpiredParameter () {
    const query = 'SELECT "RemoveExpiredService" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros"';
    
    const statement = oConn.prepareStatement(query);

	const result = statement.executeQuery();

	while (result.next()) {
		return result.getNString(1);
	}

	return 0;
} 

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
        statement.setString(3, 'Interface de Encerramento');
        statement.setString(4, 'Pré-Agrupamento encerrado pela interface de encerramento automático.');
        
        let information = '';
        information +=   'Status anterior: ' + protocol.Status;
        information += '\nStatus pós encerramento: ' + getStatus(protocol);
        information += '\nToneladas restantes: ' + parseFloat(protocol.ToneladasRestantes).toFixed(2);
        information += '\nToneladas iniciais: ' + parseFloat(protocol.ToneladasRestantesInicial).toFixed(2);
        
        statement.setString(5, information);
        
        statement.addBatch();
    });
    
    statement.executeBatch();
    oConn.commit();
};

function isNowBetweenMidnightAndInicioCelula(now, inicioCelula) {
    const celulaHour = parseInt(inicioCelula.split(':')[0], 10);
    const celulaMinute = parseInt(inicioCelula.split(':')[1], 10);
    
    if(now.getHours() < celulaHour) {
        return true;
    }
    
    if(now.getHours() === celulaHour && now.getMinutes() < celulaMinute ) {
        return true;
    }
    
    return false;
}

function getProtocolsToDeleteToday (Parametros) {
    let query = ' SELECT "NumAgrupamento", "Status", "SLA_OTIF", "Excluido", (SELECT "HoraLimiteInterface" FROM "COMP_CARGA"."comp_carga.table::cds_table.Parametros") AS "HoraLimiteInterface",';
    query +=    ' "ToneladasRestantes", "ToneladasRestantesInicial", "MinimaToneladasRestante", "InterfaceAtuante"';
    query +=    ' FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo"';
    query +=    ' WHERE ("Status" = \'Pausada\' OR "Status" = \'Aguardando\')';
    
    const oStmt = oConn.prepareStatement(query);
    
    const oResultSet = oStmt.executeQuery();
    
    const aProtocols = [];
    const iCarregamento = Parametros.TMAC / 60;
    const now = new Date(new Date().setHours(new Date().getHours() - 3));
    const ISODate = now.toISOString().split('T')[0];
    
    let CelulaInicio = new Date(ISODate + 'T' + Parametros.InicioCelula + ':00'); 
    let CelulaFim = new Date(ISODate + 'T' + Parametros.FimCelula + ':00');

    const isBetween = isNowBetweenMidnightAndInicioCelula(now, Parametros.InicioCelula);
    
    if(isBetween) {
        CelulaInicio = new Date( CelulaInicio.setDate(CelulaInicio.getDate() - 1) );
        CelulaFim = new Date( CelulaFim.setDate(CelulaFim.getDate() - 1) );
    }
    
    while (oResultSet.next()) {
        let sla = new Date(oResultSet.getTimestamp(3));
        let slaCopy = new Date(oResultSet.getTimestamp(3));
        
        let slaTMAC = new Date(slaCopy.setHours(slaCopy.getHours() - iCarregamento));
        
        if (now > CelulaInicio && now < CelulaFim) {
            if (slaTMAC < now) {
                aProtocols.push({
                    NumAgrupamento: oResultSet.getString(1),
                    Status: oResultSet.getString(2),
                    Delete: false,
                    Excluido: oResultSet.getString(4),
                    ToneladasRestantes: oResultSet.getString(6),
                    ToneladasRestantesInicial: oResultSet.getString(7),
                    MinimaToneladasRestante: oResultSet.getString(8),
                    InterfaceAtuante: oResultSet.getString(9)
                });
            } 
        } else {
            let HoraLimiteInterface = new Date(new Date(now.toISOString()).setHours(CelulaFim.getHours() + oResultSet.getInteger(5)));

            if(isBetween) {
                HoraLimiteInterface = new Date( HoraLimiteInterface.setDate(HoraLimiteInterface.getDate() - 1) );
            }
            
            if (sla < HoraLimiteInterface) {
                aProtocols.push({
                    NumAgrupamento: oResultSet.getString(1),
                    Status: oResultSet.getString(2),
                    Delete: false,
                    Excluido: oResultSet.getString(4),
                    ToneladasRestantes: oResultSet.getString(6),
                    ToneladasRestantesInicial: oResultSet.getString(7),
                    MinimaToneladasRestante: oResultSet.getString(8),
                    InterfaceAtuante: oResultSet.getString(9)
                });
            }
        }
    }
    
    const protocolsToDelete = aProtocols.filter(function (protocol) {
        return protocol.Delete;
    });
    
    removeProtocols(protocolsToDelete.map(function (protocol) {
        return {
            NumAgrupamento: protocol.NumAgrupamento,
            Status: protocol.Status,
            Excluido: protocol.Delete ? 'X' : ''
        };
    }));
    
    const protocolsToEnd = aProtocols.filter(function (protocol) {
        return !protocol.Delete;
    });
    
    updateExpiredProtocols(protocolsToEnd);
    
    return aProtocols;
}

function mainFunction() {
    // var a = getExpiredProtocols();
    $.response.status = $.net.http.OK;
    $.response.contentType = "application/json";
    // $.response.setBody( JSON.stringify({
    //     protocols: a,
    //     date: new Date()
    // }) );
    try {
        let removeExpiredService = getRemoveExpiredParameter();
        
        $.response.setBody(JSON.stringify(removeExpiredService));
        
        if (!removeExpiredService) {
            $.response.status = $.net.http.OK;
            $.response.contentType = "application/json";
            $.response.setBody(JSON.stringify(getResponseData([])));
            return;
        }
        
        let oParametros = getParametros();
        
        if (!oParametros.TMAC) {
            $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
            $.response.contentType = "text/html";
            $.response.setBody("No 'TempoCarregamentoEmMinutos' for 'Parametros' table.");
            return;
        }
        
        // let aProtocols = getExpiredProtocols(oParametros);
         
        // if(aProtocols.length > 0) {
        //     updateExpiredProtocols(aProtocols);
        // }
        
        // const aDeletedProtocols = deleteProtocols(); // RF07 - Pregrupamentos com "Complemento Parcial" ou "Não Complementado", que tiverem clientes com SLA de OTIF para o dia D+1, D+2, D+3 devem ser excluídos.
        const aProtocolsToDeleteToday = getProtocolsToDeleteToday(oParametros);
        // const sendData = removeDuplicates(
        //     removeDuplicates(aProtocols, aDeletedProtocols),
        //     aProtocolsToDeleteToday);
        // const sendData = removeDuplicates(aProtocolsToDeleteToday/*, aDeletedProtocols*/);
        
        const sendData = aProtocolsToDeleteToday;
        
        insertProtocolRollbackLog(sendData);
        upsertLog(sendData);
        
        $.response.status = $.net.http.OK;
        $.response.contentType = "application/json";
        $.response.setBody(JSON.stringify(getResponseData(sendData)));
        
    } catch(e) {
        $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
        $.response.contentType = "text/html";
        $.response.setBody( e.message );

    }
}

deleteRollbackLog();
mainFunction();
oConn.close();