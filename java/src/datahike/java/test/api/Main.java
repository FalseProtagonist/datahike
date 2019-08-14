package datahike.java.test.api;

import datahike.java.Util;
import datahike.java.Datahike;
import static datahike.java.Util.k;

// To run it: java -cp target/datahike-0.2.0-beta3-standalone.jar datahike.java.test.api.Main
public class Main {
    public static void main(String[] args) {
        // Transacting new schema
        String uri = "datahike:mem://test-empty-db";
        Datahike.deleteDatabase(uri);
        Datahike.createDatabase(uri);
        Object conn = Datahike.connect(uri);
        Datahike.transact(conn, Util.vector(Util.map(k(":db/ident"), k(":name"),
                                                k(":db/valueType"), k(":db.type/string"),
                                                k(":db/cardinality"), k(":db.cardinality/one"))));
        System.out.println("Done!");

        // Transacting with schema present
        Datahike.transact(conn, Util.vector(Util.map(k(":name"), "Alice")));

        // Querying
        Object ddb = Datahike.db(conn);
        
        System.out.println(ddb.getClass());
    }
}