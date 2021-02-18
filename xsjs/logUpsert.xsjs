const conn = $.db.getConnection();

const upsert = function (data) {
    const query = 'UPSERT "COMP_CARGA"."comp_carga.table::cds_table.LOG_CompleCarga" ("NumAgrupamento","Data","Vendedor","Descricao","Informacao") VALUES (?, ?, ?, ?, ?) WITH PRIMARY KEY';
    
    const statement = conn.prepareStatement(query);
    
    statement.setString(1, data.NumAgrupamento);
    statement.setString(2, data.Data);
    statement.setString(3, data.Vendedor);
    statement.setString(4, data.Descricao);
    statement.setString(5, data.Informacao);
    
    statement.executeQuery();
    conn.commit();
};

try {
    const NumAgrupamento = $.request.parameters.get('NumAgrupamento');
    const Data = $.request.parameters.get('Data');
    const Vendedor = $.request.parameters.get('Vendedor');
    const Descricao = $.request.parameters.get('Descricao');
    const Informacao = $.request.parameters.get('Informacao');
    
    upsert({
        NumAgrupamento: NumAgrupamento,
        Data: Data,
        Vendedor: Vendedor,
        Descricao: Descricao,
        Informacao: Informacao
    });
    
    $.response.status = $.net.http.OK;
    $.response.contentType = "text/html";
    $.response.setBody(JSON.stringify({
        status: $.net.http.OK,
        data: {
            NumAgrupamento: NumAgrupamento,
            Data: Data,
            Vendedor: Vendedor,
            Descricao: Descricao,
            Informacao: Informacao
        }
    }));
} catch (err) {
    conn.close();
    $.response.status = $.net.http.BAD_REQUEST;
    $.response.contentType = "text/html";
    $.response.setBody(JSON.stringify({
        status: $.net.http.BAD_REQUEST,
        data: {
            NumAgrupamento: NumAgrupamento,
            Data: Data,
            Vendedor: Vendedor,
            Descricao: Descricao,
            Informacao: Informacao
        }
    }));
}

conn.close();