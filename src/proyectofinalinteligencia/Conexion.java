/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proyectofinalinteligencia;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author dsantillanes
 */
public class Conexion {
private static Conexion INSTANCE;
    private Connection con;
    private final static String USUARIO = "SYSTEM";
    private final static String PASSWORD = "123456";
    private final static String CONEXION = "jdbc:oracle:thin:@localhost:1521/xe";

    private Conexion() {
        initConnection();
    }
    
    private void initConnection(){
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection(CONEXION, USUARIO, PASSWORD);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static Conexion getInstance(){
        if(INSTANCE == null){
            INSTANCE = new Conexion();
        }
        return INSTANCE;
    }
    
    /**
     * @return the con
     */
    public Connection getCon() {
        return con;
    }
}
