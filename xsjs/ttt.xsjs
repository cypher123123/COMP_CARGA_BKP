// let strComplementos = $.request.parameters.get("complementos");
// // let arrComplementos = strComplementos.split("/");

// const conn = $.db.getConnection();
// let query = 'INSERT INTO "COMP_CARGA"."comp_carga.table::cds_table.LOG_ResetedProtocols" ("Time", "Input") VALUES(?, ?)';
// let stmt = conn.prepareStatement(query);

// stmt.setTimestamp(1, new Date(new Date().setHours(new Date().getHours() - 3)));
// stmt.setString(2, strComplementos);
// stmt.executeUpdate();
// conn.commit();

// $.response.status = $.net.http.OK;
// $.response.contentType = "application/json";
// $.response.setBody(JSON.stringify({teste: strComplementos}));  
