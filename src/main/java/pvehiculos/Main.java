package pvehiculos;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.TypedQuery;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class Main {
    String driver = "jdbc:postgresql:";
    String host = "//localhost:";
    String porto = "5432";
    String sid = "postgres";
    String usuario = "dam2a";
    String password = "castelao";
    String url = driver + host + porto + "/" + sid;
    Connection conn;

    {
        try {
            conn = DriverManager.getConnection(url, usuario, password);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Método de conexión que devuelve un objeto de tipo Connection para usar con los métodos a base de datos
    public Connection conexion() throws SQLException {

        return conn;
    }

    public void desconectar() {
        conn = null;
    }

    public static Statement stat;

    public void pvehiculos() throws SQLException {
        //Conexion con el statement
        stat = conn.createStatement();
        // Creating a Mongo client
        MongoClient mongo = MongoClients.create("mongodb://localhost:27017");
        System.out.println("Connected to the database successfully");
        // Accessing the database
        MongoDatabase database = mongo.getDatabase("test");
        // Retrieving a collection
        MongoCollection<Document> collection = database.getCollection("vendas");
        System.out.println("Collection sampleCollection selected successfully");
        System.out.println("==================================================================");

        FindIterable<Document> docs = collection.find();
        MongoCursor<Document> iterator = docs.iterator();
          /*
        Con esto haces una "consulta" donde aÃ±ades los campos que quieres que aparezcan
        y los que quieres excluir, luego haces una iteracion para que te salgan todos
        */
      /*  Bson projectionFields = Projections.fields(
                Projections.include("id", "dni", "codveh"));
        try (MongoCursor<Document> cursor = collection.find()
                .projection(projectionFields)
                .sort(Sorts.descending("id")).iterator()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        }*/

        //CONEXION OBJECTDB
        EntityManagerFactory emf =
                Persistence.createEntityManagerFactory(
                        "/home/dam2a/Escritorio/AD/ExamenPVehiculos/vehicli.odb");
        EntityManager em = emf.createEntityManager();

        while (iterator.hasNext()) {
            Document doc = iterator.next();
            int _id = doc.getInteger("_id");
            String dni = doc.getString("dni");
            String codveh = doc.getString("codveh");
            // amosar datos da coleccion vendas
            System.out.println("_id: " + _id + ",\tdni: " + dni + ",\tcodveh: " + codveh);

            TypedQuery<Clientes> query = em.createQuery("SELECT c FROM Clientes c WHERE c.dni = '" + dni + "'", Clientes.class);
            List<Clientes> cliente = query.getResultList();
            for (Clientes c : cliente) {
                // amosar o nome e o numero de compras do cliente correspondente
                System.out.println("nomc: " + c.nomec + ", ncompras: " + c.ncompras);

            }

            TypedQuery<Vehiculos> query2 = em.createQuery("SELECT v FROM Vehiculos v WHERE v.codveh = '" + codveh + "'", Vehiculos.class);
            List<Vehiculos> vehiculo = query2.getResultList();
            for (Vehiculos v : vehiculo) {
                // amosar o nome, anomatricula e prezo orixe do vehiculo correspondente
                System.out.println("nomveh: " + v.nomveh + ", ano matricula: " + v.anomatricula + ", prezo orixe: " + v.prezoorixe);

            }

            int desconto = 0;

            for (Clientes c : cliente) {
                for (Vehiculos v : vehiculo) {
                    if (c.ncompras >= 1) {
                        desconto = 500;
                    }
                    //facer calculo correspondente do prezo final do vehiculo en funcion de si ten dereito ou non a desconto
                    int pf = v.prezoorixe - ((2019 - v.anomatricula) * 500) - desconto;
                    System.out.println("prezoFinal: " + pf);

                    //Inserir a fila correspondiente na taboa finalveh
                   stat.executeUpdate("INSERT INTO FINALVEH (id,dni,nomec,vehf) VALUES "
                            + "(" + _id + ", '" + dni + "', '" + c.nomec + "',('" + v.nomveh + "'," + pf + ")"
                            + ")");
                }
                System.out.println("==================================================================");

            }

        }
        iterator.close();
        em.close();
        emf.close();
    }

    public static void main(String[] args) throws SQLException {
        Main m = new Main();
        m.pvehiculos();
    }

}