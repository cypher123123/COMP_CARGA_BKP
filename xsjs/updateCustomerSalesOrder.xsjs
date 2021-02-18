let conn = $.db.getConnection();

function isRequestedMethodAllowed() {
    if($.request.method === $.net.http.POST) {
        return true;
    }
    
    $.response.status = $.net.http.METHOD_NOT_ALLOWED;
    $.response.contentType = 'text/html';
    $.response.setBody('Only POST method is allowed');
    
    return false;
}

function upsertLog(protocols) {
    if (protocols.length === 0) {
        return;
    }
    
    const query = 'UPSERT "COMP_CARGA"."comp_carga.table::cds_table.LOG_CompleCarga" ("NumAgrupamento","Data","Vendedor","Descricao","Informacao") VALUES (?, ?, ?, ?, ?) WITH PRIMARY KEY';
    
    const statement = conn.prepareStatement(query);
    
    statement.setBatchSize(protocols.length);
    
    protocols.forEach(function (protocol) {
        statement.setString(1, protocol.NumAgrupamento);
        statement.setString(2, new Date().toISOString());
        statement.setString(3, 'Interface de Ordem de Venda');
        statement.setString(4, 'Pré-Agrupamento encerrado após a subida ordem de venda pela interface.');
        statement.setString(5, '');
        
        statement.addBatch();
    });
    
    statement.executeBatch();
    conn.commit();
};

function doesOrderHasParent (NumAgrupamento) {
    let query = '';
    query += ' SELECT "NumAgrupamento" FROM "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" ';
    query += ' WHERE "NumAgrupamento" = ? ';
    
    let statement = conn.prepareStatement(query);
    
    statement.setString(1, NumAgrupamento);
    
    let record = statement.executeQuery();
    
    while (record.next()) {
        return true;
    }
    
    return false;
}
    
function addSalesOrders(list) {
    let query = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.OrdemVenda" ';
    query += '("NumAgrupamento", "IdCliente", "OrdemVenda", "ToneladasOV", "OrdemDoComplemento", "DataOrdemVenda") VALUES(?, ?, ?, ?, 1, ?)';
    // OrdemDoComplemento = 1 significa que essa ordem veio do ECC
    
    try {
        let stmt = conn.prepareStatement(query);
        stmt.setBatchSize(list.length);
        
        let hasOrderToAdd = false;
        
        list.forEach(function(item) {
            
            let hasParent = doesOrderHasParent(item.NrComplemento);
            
            if (!hasParent) {
                return;
            }
            
            stmt.setString(1, item.NrComplemento);
            stmt.setString(2, item.Cliente);
            stmt.setString(3, item.OrdemVendas);
            stmt.setDouble(4, parseFloat(item.PesoTotal));
            stmt.setTimestamp(5, new Date(new Date().setHours(new Date().getHours() - 3)));
            
            stmt.addBatch();
            hasOrderToAdd = true;
            
        });
        
        if (!hasOrderToAdd) {
            return 0;
        }
        
        let records = stmt.executeBatch();
        let sumOfRecords = 0;
        
        records.forEach(function(record) {
            sumOfRecords += record;
        });
        
        return sumOfRecords;
    } catch(e) {
        return e.message;
    }
    
}

function getToneladasRestantes(list) {
    let query = 'SELECT "NumAgrupamento", "ToneladasRestantes" FROM "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    query += 'WHERE ';
    
    try {
        list.forEach(function(item, i) {
            if(i === 0) {
                query += '"NumAgrupamento" = ? ';
            }
            else {
                query += 'OR "NumAgrupamento" = ? ';
            }
        });
        
        let stmt = conn.prepareStatement(query);
        
        list.forEach(function(item, i) {
            stmt.setString(i + 1, item.NrComplemento);
        });
        
        let resultSet = stmt.executeQuery();
        let protocolos = [];
        
        while(resultSet.next()) {
            protocolos.push({
                NumAgrupamento: resultSet.getString(1),
                ToneladasRestantes: resultSet.getDouble(2)
            });
        }
        
        return protocolos;
    } catch(e) {
        return e.message;
    }
}

function updateToneladasRestantesAndFrete(list) {
    // let query = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    // query += 'SET "ToneladasRestantes" = ?, "PalletsRestantes" = ? ';
    // query += 'WHERE "NumAgrupamento" = ? ';
    
    let query = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" ';
    query += 'SET "ToneladasRestantes" = ?, "PalletsRestantes" = ?, "FreteMorto" = ? ';
    query += 'WHERE "NumAgrupamento" = ? ';
    
    let protocolos = getToneladasRestantes(list);
    
    try {
        let stmt = conn.prepareStatement(query);
        stmt.setBatchSize(protocolos.length);
        
        let hasToneladasToUpdate = false;
        
        protocolos.forEach(function(protocolo) {
            
            let hasParent = doesOrderHasParent(protocolo.NumAgrupamento);
            
            if (!hasParent) {
                return;
            }
            
            const orders = list
                .filter(function (item) {
                    return protocolo.NumAgrupamento = item.NrComplemento;
                })
                .sort(function (a, b) {
                    return parseFloat(a.FreteMorto) - parseFloat(b.FreteMorto);
                })
            
            let FreteMorto = parseFloat(orders[0].FreteMorto);
            
            const pesoTotal = orders.reduce(function (acc, order) {
                acc += parseFloat(order.PesoTotal);
                return acc;
            }, 0)
            
            let tonRestante = parseFloat(protocolo.ToneladasRestantes) - pesoTotal;
            let palletRestante = Math.round(tonRestante / 2);
            
            // stmt.setDouble(1, tonRestante);
            // stmt.setInteger(2, palletRestante);
            // stmt.setString(3, protocolo.NumAgrupamento);
            
            stmt.setDouble(1, tonRestante);
            stmt.setInteger(2, palletRestante);
            stmt.setDouble(3, FreteMorto);
            stmt.setString(4, protocolo.NumAgrupamento);
            
            stmt.addBatch();
            hasToneladasToUpdate = true;
        });
        
        if (!hasToneladasToUpdate) {
            return 0;
        }
        
        let records = stmt.executeBatch();
        let sumOfRecords = 0;
        
        records.forEach(function(record) {
            sumOfRecords += record;
        });
        
        return sumOfRecords;
    } catch(e) {
        return e.message;
    }
    
}

function updateCustomers(list) {
    let query = 'UPDATE "COMP_CARGA"."comp_carga.table::cds_table.ProtocoloCliente" ';
    query += 'SET "Classificacao" = ? ';
    query += 'WHERE "NumAgrupamento" = ? AND "IdCliente" = ?';
    
    try {
        let stmt = conn.prepareStatement(query);
        stmt.setBatchSize(list.length);
        
        let hasCustomerToAdd = false;
        
        list.forEach(function(item) {
            
            let hasParent = doesOrderHasParent(item.NrComplemento);
            
            if (!hasParent) {
                return;
            }
            
            stmt.setString(1, "A");
            stmt.setString(2, item.NrComplemento);
            stmt.setString(3, item.Cliente);
            stmt.addBatch();
            hasCustomerToAdd = true;
        });
        
        if (!hasCustomerToAdd) {
            return 0;
        }
        
        let records = stmt.executeBatch();
        let sumOfRecords = 0;
        
        records.forEach(function(record) {
            sumOfRecords += record;
        });
        
        return sumOfRecords;
    } catch(e) {
        return e.message;
    }
    
}

function updateStatus(list) {
    let query = `UPDATE "COMP_CARGA"."comp_carga.table::cds_table.Protocolo" 
                 SET "Status" = ?, "AtendimentoFim" = ?, "InterfaceAtuante" = 'Ordem de Venda'
                 WHERE "NumAgrupamento" = ?`;
    
    const now = new Date(new Date().setHours(new Date().getHours() - 3));
    
    try {
        let stmt = conn.prepareStatement(query);
        stmt.setBatchSize(list.length);
        
        let hasToBeUpdated = false;
        let aProtocolsToUpdate = [];
        
        list.forEach(function(item) {
            // let Status = item.Complementado && item.Complementado === 'X' ? 'Complemento Total' : 'Pausada';
            // Status = Status ? Status : 'Aguardando';
            
            let hasParent = doesOrderHasParent(item.NrComplemento);
            
            if (!hasParent) {
                return;
            }
            
            if (item.Complementado !== 'X') {
                return;
            }
            
            stmt.setString(1, 'Complemento Total');
            stmt.setTimestamp(2, now);
            stmt.setString(3, item.NrComplemento);
            stmt.addBatch();
            hasToBeUpdated = true;
            aProtocolsToUpdate.push({ 
                NumAgrupamento: item.NrComplemento 
            });
        });
        
        if (!hasToBeUpdated) {
            return 0;
        }
        
        let records = stmt.executeBatch();
        let sumOfRecords = 0;
        
        records.forEach(function(record) {
            sumOfRecords += record;
        });
        
        upsertLog(aProtocolsToUpdate);
        
        return sumOfRecords;
    } catch(e) {
        return e.message;
    }
}

function mainFunction() {
    if(isRequestedMethodAllowed()) {
        
        try {
            let body = $.request.body.asString();
            let jsonBody = JSON.parse(body);
            // let jsonBody = {
            //     "d": {
            //         "results": [
            //             {
            //                 "NrComplemento": "20810",
            //                 "Cliente": "0001283506",
            //                 "OrdemVendas": "1111",
            //                 "PesoTotal": "10",
            //                 "Complementado": "",
            //                 "FreteMorto": "109"
            //             }
            //         ]
            //     }
            // };
            
            let results = jsonBody.d.results;
            
            if(results.length > 0) {
                let insertedSalesOrders = addSalesOrders(results);
                let updatedProtocols = updateToneladasRestantesAndFrete(results);
                let updatedCustomers = updateCustomers(results);
                let updatedProtocolsStatus = updateStatus(results);
                
                conn.commit();
                $.response.status = $.net.http.OK;
                $.response.contentType = 'application/json';
                $.response.setBody(JSON.stringify({
                    InsertedSalesOrders: insertedSalesOrders,
                    UpdatedProtocols: updatedProtocols,
                    UpdatedCustomers: updatedCustomers,
                    UpdatedProtocolsStatus: updatedProtocolsStatus
                }));
            }
            else {
                $.response.status = $.net.http.OK;
                $.response.contentType = 'text/html';
                $.response.setBody('No data to be inserted');
            }
            
        } catch (e) {
            $.response.status = $.net.http.INTERNAL_SERVER_ERROR;
            $.response.contentType = 'text/html';
            $.response.setBody('Error: ' + e.message);
        }
    }
}

mainFunction();
conn.close();