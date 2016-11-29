/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proyectofinalinteligencia;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import javax.management.Query;

/**
 *
 * @author dsantillanes
 */
public class Consulta {
    
    public static String CONSULTA1;
    public static String CONSULTA2;
    public static String CONSULTA3;
    public static String CONSULTA4;
    public static String CONSULTA5;
    public static String CONSULTA6;
    public static String CONSULTA7;
    public static String CONSULTA8;
    public static String CONSULTA9;
    public static String CONSULTA10;

    public static final String DEFAULT_DATE_FORMAT = "dd/MM/yyyy";

    //Ventas totales por dia
    public static ResultSet consulta1(Date date) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA1 = String.format("SELECT SUM(v.SALE_TOTAL) TOTAL_VENDIDO_POR_DIA \n"
                + "FROM Ventas v"
                + "FULL OUTER JOIN TIEMPO t"
                + "ON t.DATE = v.DATE"
                //                + "WHERE t.DATE = CURRENT_TIMESTAMP";
                + "WHERE t.DATE = TO_DATE('%s', '%s')", DEFAULT_DATE_FORMAT, sdf.format(date));
        return st.executeQuery(CONSULTA1);

    }

    //Productos vendidos por dia.
    public static ResultSet consulta2(Date date) throws SQLException {
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA2 = String.format("SELECT p.PRODUCT_NAME, COUNT(p.PRODUCT_ID)"
                + "FROM PRODUCTOS p"
                + "FULL OUTER JOIN TIEMPO t"
                + "FULL OUTER JOIN VENTAS v"
                + "ON t.FECHA = v.FECHA"
                + "ON p.PRODUCT_ID = v.PRODUCT_ID"
                //                + "WHERE t.DATE = CURRENT_TIMESTAMP"
                + "WHERE t.DATE = %s"
                + "GROUP BY ROLLUP (p.PRODUCT_NAME)", date);
        return st.executeQuery(CONSULTA2);
    }

    // Ventas por almacen
    public static ResultSet consulta3(Integer numAlmacen) throws SQLException {
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA3 = "SELECT SUM(v.SALE_TOTAL) TOTAL_VENDIDO_POR_DIA \n"
                + "FROM Ventas v"
                + "FULL OUTER JOIN AlMACENES a"
                + "ON a.WAREHOUSE_ID = v.WAREHOUSE_ID"
                + "WHERE a.WAREHOUSE_ID = %s";
        return st.executeQuery(CONSULTA3);
    }

    //Clientes con mas ventas
    public static ResultSet consulta4() throws SQLException {
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA4 = "SELECT CLIENT_NAME c, SUM(v.SALE_TOTAL) TOTAL_VENDIDO"
                + "FROM CLIENTES c"
                + "FULL OUTER JOIN VENTAS v"
                + "ON c.CLIENT_ID = v.CLIENT_ID"
                //                + "WHERE c.CLIENT_ID = %s"
                + "ORDER BY SUM(v.SALE_TOTAL) DESC";
        return st.executeQuery(CONSULTA4);
    }

    //Categorias de productos mas vendidos por mes.
    public static ResultSet consulta5(Integer numMes) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA5 = "SELECT p.CATEGORY, COUNT(v.PRODUCT_ID) "
                + "FROM VENTAS v "
                + "JOIN PRODUCTOS p "
                + "JOIN TIEMPO t"
                + "ON v.PRODUCT_ID = p.PRODUCT_ID "
                + "ON v.DATE = t.DATE"
                + "WHERE t.MONTH_NUMBER = %s"
                + "GROUP BY GROUPING SETS(p.CATEGORY) "
                + "ORDER BY COUNT(v.PRODUCT_ID) DESC";
        return st.executeQuery(String.format(CONSULTA5,numMes));
    }

    //Ventas totales por año
    public static ResultSet consulta6(Integer year) throws SQLException {
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA6 = "SELECT SUM(v.SALES_TOTAL) "
                + "FROM VENTAS v "
                + "JOIN TIEMPO t "
                + "ON v.DATE = t.DATE "
                + "WHERE t.YEAR = %s";
        return st.executeQuery(String.format(CONSULTA6,year));
    }

    //Productos con menos ventas por año.
    public static ResultSet consulta7(Integer year) throws SQLException {
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA7 = "SELECT SELECT p.PRODUCT_NAME, COUNT(v.PRODUCT_ID) "
                + "FROM VENTAS v "
                + "JOIN PRODUCTOS p "
                + "JOIN TIEMPO t"
                + "ON v.PRODUCT_ID = p.PRODUCT_ID "
                + "ON v.DATE = t.DATE"
                + "WHERE t.YEAR = %s"
                + "GROUP BY GROUPING SETS(p.PRODUCT_NAME) "
                + "ORDER BY COUNT(v.PRODUCT_ID) ASC";
        return st.executeQuery(String.format(CONSULTA7,year));
    }

    //Ventas totales entre un periodo de fecha.
    public static ResultSet consulta8(Date desde, Date hasta) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA8 = "SELECT SUM(v.SALES_TOTAL) "
                + "FROM VENTAS v "
                + "JOIN PRODUCTOS p "
                + "ON v.PRODUCT_ID = p.PRODUCT_ID "
                + "WHERE TO_DATE(v.FECHA, '%s') BETWEEN TO_DATE('%s', '%s') "
                + "AND TO_DATE('%s', '%s') ";
        return st.executeQuery(String.format(CONSULTA8,
                DEFAULT_DATE_FORMAT,
                sdf.format(desde),
                DEFAULT_DATE_FORMAT,
                sdf.format(hasta),
                DEFAULT_DATE_FORMAT));
    }

    //Productos por almacen
    public static ResultSet consulta9(Integer idAlmacen) throws SQLException {
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA9 = "SELECT SELECT p.PRODUCT_NAME, COUNT(v.PRODUCT_ID) "
                + "FROM VENTAS v"
                + "JOIN PRODUCTOS p"
                + "JOIN ALAMECES a"
                + "ON v.PRODUCT_ID = p.PRODUCT_ID "
                + "ON v.WAREHOUSE_ID = a.WAREHOUSE_ID"
                + "WHERE a.WAREHOUSE_ID= %s"
                + "GROUP BY GROUPING SETS(p.PRODUCT_NAME) "
                + "ORDER BY COUNT(v.PRODUCT_ID) DESC";
        return st.executeQuery(String.format(CONSULTA9,idAlmacen));
    }

    //Almacen con menos ventas
    public static ResultSet consulta10() throws SQLException {
        Statement st = Conexion.getInstance().getCon().createStatement();
        CONSULTA10 = "SELECT WAREHOUSE_NAME a, SUM(v.SALE_TOTAL) TOTAL_VENDIDO"
                + "FROM ALMACENES a"
                + "FULL OUTER JOIN VENTAS v"
                + "ON a.WAREHOUSE_ID = v.WAREHOUSE_ID"
                + "ORDER BY SUM(v.SALE_TOTAL) ASC";
        return st.executeQuery(CONSULTA10);
    }

}
