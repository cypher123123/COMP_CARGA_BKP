let query = 'SELECT * FROM "COMP_CARGA"."comp_carga.table::cds_table.LOG_InsertedDataFromCPI" ORDER BY "Time" DESC';

let conn = $.db.getConnection();
let pstmt = conn.prepareStatement(query);
let rs = pstmt.executeQuery();
let response = [];

while(rs.next()) {
    response.push({
            Time: rs.getTimestamp(1),
            Status: rs.getString(2),
            Json: rs.getString(3),
            Protocolos: rs.getString(4),
            Clientes: rs.getString(5),
            ProtocoloClientes: rs.getString(6),
            OrdensVenda: rs.getString(7),
            Telefones: rs.getString(8),
            Compras: rs.getString(9),
            ComprasItens: rs.getString(10)
        });
}

$.response.status = $.net.http.OK;
$.response.contentType = 'application/json';
$.response.setBody(JSON.stringify(response));