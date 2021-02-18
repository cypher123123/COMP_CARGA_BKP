let query = 'SELECT * FROM "COMP_CARGA"."comp_carga.table::cds_table.LOG_RemoveExpiredProtocols" ORDER BY "Time" DESC';

let conn = $.db.getConnection();
let pstmt = conn.prepareStatement(query);
let rs = pstmt.executeQuery();
let response = [];

while(rs.next()) {
    response.push({
        Time: rs.getTimestamp(1),
        Json: rs.getString(2)
    });
}

$.response.status = $.net.http.OK;
$.response.contentType = 'application/json';
$.response.setBody(JSON.stringify(response));