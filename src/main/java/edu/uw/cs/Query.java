package edu.uw.cs;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.security.*;
import java.security.spec.*;
import javax.crypto.*;
import javax.crypto.spec.*;
import javax.xml.bind.*;

/**
 * Runs queries against a back-end database
 */
public class Query {
  // DB Connection
  private Connection conn;
  
  private boolean login;
  private String user;
  private HashMap<Integer, String[]> itineraries; 

  // Password hashing parameter constants
  private static final int HASH_STRENGTH = 65536;
  private static final int KEY_LENGTH = 128;

  // Canned queries
  private static final String CHECK_FLIGHT_CAPACITY = "SELECT capacity FROM Flights WHERE fid = ?";
  private PreparedStatement checkFlightCapacityStatement;
  // TODO: YOUR CODE HERE
  private static final String CLEAR_USERS = "DELETE FROM Users";
  private PreparedStatement clearUsersStatement;
  
  private static final String CLEAR_RESERVATIONS = "DELETE FROM Reservations";
  private PreparedStatement clearReservationsStatement;
  
  private static final String CLEAR_ID = "DELETE FROM ID";
  private PreparedStatement clearIdStatement;
  
  private static final String CLEAR_BOOKED = "DELETE FROM Booked";
  private PreparedStatement clearBookedStatement;
  
  private static final String CHECK_USER = "SELECT COUNT(*) AS cnt FROM Users WHERE username = ?";
  private PreparedStatement checkUserStatement;
  
  private static final String CREATE_USER = "INSERT INTO Users VALUES (?, ?, ?, ?)";
  private PreparedStatement createUserStatement;
  
  private static final String CHECK_LOGIN = "SELECT password_hash, password_salt FROM Users WHERE username = ?";
  private PreparedStatement checkLoginStatement;
  
  private static final String NEW_ID = "INSERT INTO ID VALUES (1)";
  private PreparedStatement newIdStatement;
  
  private static final String INSERT_RESERVATION = "INSERT INTO Reservations VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  private PreparedStatement insertReservationStatement;
  
  private static final String UPDATE_ID = "UPDATE ID SET next_id = ?";
  private PreparedStatement updateIdStatement;
  
  private static final String RESERVATION_PRICE = "SELECT total_price FROM Reservations WHERE id = ? AND username = ? AND paid = 0";
  private PreparedStatement reservationPriceStatement;
  
  private static final String GET_BALANCE = "SELECT balance FROM Users WHERE username = ?";
  private PreparedStatement getBalanceStatement;
  
  private static final String UPDATE_PAID = "UPDATE Reservations SET paid = 1 WHERE id = ?";
  private PreparedStatement updatePaidStatement;
  
  private static final String UPDATE_BALANCE = "UPDATE Users SET balance = ? WHERE username = ?";
  private PreparedStatement updateBalanceStatement;
  
  private static final String GET_RESERVATIONS = "SELECT id, itinerary, paid FROM Reservations WHERE username = ? ORDER BY id";
  private PreparedStatement getReservationsStatement;
  
  private static final String PRICE_PAID = "SELECT total_price, paid, fid1, fid2 FROM Reservations WHERE id = ? AND username = ?";
  private PreparedStatement pricePaidStatement;
  
  private static final String DELETE_RESERVATION = "DELETE FROM Reservations WHERE id = ?";
  private PreparedStatement deleteReservationStatement;
  
  private static final String GET_BOOKED = "SELECT booked FROM Booked WHERE fid = ?";
  private PreparedStatement getBookedStatement;
  
  private static final String INSERT_BOOKED = "INSERT INTO Booked VALUES (?, 1)";
  private PreparedStatement insertBookedStatement;
  
  private static final String UPDATE_BOOKED = "UPDATE Booked SET booked = ? WHERE fid = ?";
  private PreparedStatement updateBookedStatement;
  
  private static final String BEGIN_TRANSAC = "BEGIN TRANSACTION;";
  private PreparedStatement beginTransacStatement;
  
  private static final String COMMIT = "COMMIT;";
  private PreparedStatement commitStatement;
  
  private static final String ROLLBACK = "ROLLBACK;";
  private PreparedStatement rollbackStatement;
  		
  /**
   * Establishes a new application-to-database connection. Uses the
   * dbconn.properties configuration settings
   * 
   * @throws IOException
   * @throws SQLException
   */
  public void openConnection() throws IOException, SQLException {
    // Connect to the database with the provided connection configuration
    Properties configProps = new Properties();
    configProps.load(new FileInputStream("dbconn.properties"));
    String serverURL = configProps.getProperty("hw1.server_url");
    String dbName = configProps.getProperty("hw1.database_name");
    String adminName = configProps.getProperty("hw1.username");
    String password = configProps.getProperty("hw1.password");
    String connectionUrl = String.format("jdbc:sqlserver://%s:1433;databaseName=%s;user=%s;password=%s", serverURL,
        dbName, adminName, password);
    conn = DriverManager.getConnection(connectionUrl);

    // By default, automatically commit after each statement
    conn.setAutoCommit(true);

    // By default, set the transaction isolation level to serializable
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
  }

  /**
   * Closes the application-to-database connection
   */
  public void closeConnection() throws SQLException {
    conn.close();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try {
        // TODO: YOUR CODE HERE
        clearIdStatement.executeUpdate();      
        clearReservationsStatement.executeUpdate();
        clearUsersStatement.executeUpdate();
        clearBookedStatement.executeUpdate();
        
        // success, commit
        commitStatement.executeUpdate();
        conn.setAutoCommit(true);
      } catch (Exception ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }

  /*
   * prepare all the SQL statements in this method.
   */
  public void prepareStatements() throws SQLException {
    checkFlightCapacityStatement = conn.prepareStatement(CHECK_FLIGHT_CAPACITY);
    // TODO: YOUR CODE HERE
    clearUsersStatement = conn.prepareStatement(CLEAR_USERS);
    clearReservationsStatement = conn.prepareStatement(CLEAR_RESERVATIONS);
    clearIdStatement = conn.prepareStatement(CLEAR_ID);
    checkUserStatement = conn.prepareStatement(CHECK_USER);
    clearBookedStatement = conn.prepareStatement(CLEAR_BOOKED);
    createUserStatement = conn.prepareStatement(CREATE_USER);
    checkLoginStatement = conn.prepareStatement(CHECK_LOGIN); 
    newIdStatement = conn.prepareStatement(NEW_ID);
    insertReservationStatement = conn.prepareStatement(INSERT_RESERVATION);
    updateIdStatement = conn.prepareStatement(UPDATE_ID);
    reservationPriceStatement = conn.prepareStatement(RESERVATION_PRICE);
    getBalanceStatement = conn.prepareStatement(GET_BALANCE); 
    updatePaidStatement = conn.prepareStatement(UPDATE_PAID);
    updateBalanceStatement = conn.prepareStatement(UPDATE_BALANCE); 
    getReservationsStatement = conn.prepareStatement(GET_RESERVATIONS);
    pricePaidStatement = conn.prepareStatement(PRICE_PAID);
    deleteReservationStatement = conn.prepareStatement(DELETE_RESERVATION);
    getBookedStatement = conn.prepareStatement(GET_BOOKED);   
    insertBookedStatement = conn.prepareStatement(INSERT_BOOKED); 
    updateBookedStatement = conn.prepareStatement(UPDATE_BOOKED); 
    beginTransacStatement = conn.prepareStatement(BEGIN_TRANSAC); 
    commitStatement = conn.prepareStatement(COMMIT); 
    rollbackStatement = conn.prepareStatement(ROLLBACK); 
  }

  /**
   * Takes a user's username and password and attempts to log the user in.
   *
   * @param username user's username
   * @param password user's password
   *
   * @return If someone has already logged in, then return "User already logged
   *         in\n" For all other errors, return "Login failed\n". Otherwise,
   *         return "Logged in as [username]\n".
   */
  public String transaction_login(String username, String password) {
    // TODO: YOUR CODE HERE
    if (login == true) {
      return "User already logged in\n";
    }   

    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try {
        // get hash and salt of the given username
        checkLoginStatement.clearParameters();
        checkLoginStatement.setString(1, username);
        ResultSet results = checkLoginStatement.executeQuery();
        
        // read-only, commit
        commitStatement.executeUpdate();
        conn.setAutoCommit(true);
        
        if (results.next()) { // there is such user 
          byte[] hash = results.getBytes("password_hash");
          byte[] salt = results.getBytes("password_salt");
          results.close();
          
          // Specify the hash parameters
          KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);
          // Generate the hash
          SecretKeyFactory factory = null;
          byte[] passedHash = null;
          try {
            factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            passedHash = factory.generateSecret(spec).getEncoded();
          } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
            throw new IllegalStateException();
          }
          
          if (Arrays.equals(hash, passedHash)) { // password correct
            login = true;
            user = username;
            itineraries = null;    
            return "Logged in as " + username + "\n";
          }      
        }
       
        return "Login failed\n";
      } catch (SQLException ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
          return transaction_login(username, password);
        } catch (Exception e) {
          e.printStackTrace();
          return "Login failed\n";
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return "Login failed\n";
    }
  }

  /**
   * Implement the create user function.
   *
   * @param username   new user's username. User names are unique the system.
   * @param password   new user's password.
   * @param initAmount initial amount to deposit into the user's account, should
   *                   be >= 0 (failure otherwise).
   *
   * @return either "Created user {@code username}\n" or "Failed to create user\n"
   *         if failed.
   */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    // TODO: YOUR CODE HERE
    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try {
        // whether the user with the given username exists
        checkUserStatement.clearParameters();
        checkUserStatement.setString(1, username);
        ResultSet results = checkUserStatement.executeQuery();
        results.next();
        int count = results.getInt("cnt");
        results.close();
        
        boolean invalidBalance = (initAmount < 0);
        boolean existedUser = (count == 1);
        if (invalidBalance || existedUser) {
          // failure, rollback
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          return "Failed to create user\n";
        }
        /*
        if (invalidBalance) {
          return "Failed to create user\n";
        }
        */
        // Generate a random cryptographic salt
        SecureRandom random = new SecureRandom();
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        
        // Specify the hash parameters
        KeySpec spec = new PBEKeySpec(password.toCharArray(), salt, HASH_STRENGTH, KEY_LENGTH);

        // Generate the hash
        SecretKeyFactory factory = null;
        byte[] hash = null;
        try {
          factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
          hash = factory.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
          throw new IllegalStateException();
        }
        
        // create user with the given name, password, balance
        createUserStatement.clearParameters(); 
        createUserStatement.setString(1, username);
        createUserStatement.setBytes(2, hash);
        createUserStatement.setBytes(3, salt);
        createUserStatement.setInt(4, initAmount);
        createUserStatement.executeUpdate();
        
        // success, commit
        commitStatement.executeUpdate();
        conn.setAutoCommit(true);
        
        return "Created user " + username + "\n";
      } catch (SQLException ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
          //return "Failed to create user\n";
          return transaction_createCustomer(username, password, initAmount);
        } catch (Exception e) {
          e.printStackTrace();
          return transaction_createCustomer(username, password, initAmount);
          //return "Failed to create user\n";
        }
      } 
    } catch (Exception e) {
      e.printStackTrace();
      return "Failed to create user\n";
    }
  }

  /**
   * Implement the search function.
   *
   * Searches for flights from the given origin city to the given destination
   * city, on the given day of the month. If {@code directFlight} is true, it only
   * searches for direct flights, otherwise is searches for direct flights and
   * flights with two "hops." Only searches for up to the number of itineraries
   * given by {@code numberOfItineraries}.
   *
   * The results are sorted based on total flight time.
   *
   * @param originCity
   * @param destinationCity
   * @param directFlight        if true, then only search for direct flights,
   *                            otherwise include indirect flights as well
   * @param dayOfMonth
   * @param numberOfItineraries number of itineraries to return
   *
   * @return If no itineraries were found, return "No flights match your
   *         selection\n". If an error occurs, then return "Failed to search\n".
   *
   *         Otherwise, the sorted itineraries printed in the following format:
   *
   *         Itinerary [itinerary number]: [number of flights] flight(s), [total
   *         flight time] minutes\n [first flight in itinerary]\n ... [last flight
   *         in itinerary]\n
   *
   *         Each flight should be printed using the same format as in the
   *         {@code Flight} class. Itinerary numbers in each search should always
   *         start from 0 and increase by 1.
   *
   * @see Flight#toString()
   */
  public String transaction_search(String originCity, String destinationCity, boolean directFlight, int dayOfMonth,
      int numberOfItineraries) {
    // WARNING the below code is unsafe and only handles searches for direct flights
    // You can use the below code as a starting reference point or you can get rid
    // of it all and replace it with your own implementation.
    //
    // TODO: YOUR CODE HERE
    StringBuffer sb = new StringBuffer();
    int count = 0;
    itineraries = new HashMap<>(); 
    TreeMap<Integer, ArrayList<String[]>> sortedByTime = new TreeMap();

    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try {
        // one hop itineraries
        String oneHopSearchSQL = "SELECT TOP (" + numberOfItineraries
            + ") fid,day_of_month,carrier_id,flight_num,origin_city,dest_city,actual_time,capacity,price " + "FROM Flights "
            + "WHERE origin_city = \'" + originCity + "\' AND dest_city = \'" + destinationCity
            + "\' AND day_of_month =  " + dayOfMonth + " AND canceled <> 1 " + "ORDER BY actual_time ASC, fid ASC";

        Statement oneHopSearchStatement = conn.createStatement();
        ResultSet oneHopResults = oneHopSearchStatement.executeQuery(oneHopSearchSQL);
          
        while (oneHopResults.next()) {
          int result_fid = oneHopResults.getInt("fid");
          int result_dayOfMonth = oneHopResults.getInt("day_of_month");
          String result_carrierId = oneHopResults.getString("carrier_id");
          String result_flightNum = oneHopResults.getString("flight_num");
          String result_originCity = oneHopResults.getString("origin_city");
          String result_destCity = oneHopResults.getString("dest_city");
          int result_time = oneHopResults.getInt("actual_time");
          int result_capacity = oneHopResults.getInt("capacity");
          int result_price = oneHopResults.getInt("price");
       
          String itinerary = "ID: " + result_fid + " Day: " + result_dayOfMonth + " Carrier: " + result_carrierId + " Number: " + result_flightNum
              + " Origin: " + result_originCity + " Dest: " + result_destCity + " Duration: " + result_time
              + " Capacity: " + result_capacity + " Price: " + result_price + "\n";
          String flightDetails = ": 1 flight(s), " + result_time + " minutes\n";
          String[] itineraryDetails = {("" + result_dayOfMonth), itinerary, ("" + result_price), flightDetails, result_fid + "", "null"};
          if (!sortedByTime.containsKey(result_time)) {
            sortedByTime.put(result_time, new ArrayList());
          }
          sortedByTime.get(result_time).add(itineraryDetails);
          count++;
        }
        oneHopResults.close();
        
        int left = numberOfItineraries - count; // e.g. count = 2 means 2 direct flights
        if (directFlight != true && left >= 1) {
          String twoHopsSearchSQL = "SELECT TOP (" + left
              + ") (f1.actual_time + f2.actual_time) AS total_time,f1.fid AS fid1,f1.day_of_month AS day_of_month1,f1.carrier_id AS carrier_id1,f1.flight_num AS flight_num1,"
              + "f1.origin_city AS origin_city1,f1.dest_city AS dest_city1,f1.actual_time AS actual_time1,f1.capacity AS capacity1,f1.price AS price1," 
              + "f2.fid AS fid2,f2.day_of_month AS day_of_month2,f2.carrier_id AS carrier_id2,f2.flight_num AS flight_num2,"
              + "f2.origin_city AS origin_city2,f2.dest_city AS dest_city2,f2.actual_time AS actual_time2,f2.capacity AS capacity2,f2.price AS price2 " + "FROM Flights AS f1, Flights AS f2 "
              + "WHERE f1.dest_city = f2.origin_city AND f1.origin_city = \'" + originCity + "\' AND f2.dest_city = \'" + destinationCity
              + "\' AND f1.day_of_month =  " + dayOfMonth + " AND f1.day_of_month = f2.day_of_month" + " AND f1.canceled <> 1 AND f2.canceled <> 1 " + "ORDER BY total_time ASC, f1.fid ASC, f2.fid ASC";

          Statement twoHopsSearchStatement = conn.createStatement();
          ResultSet twoHopsResults = twoHopsSearchStatement.executeQuery(twoHopsSearchSQL);
            
          while (twoHopsResults.next()) {
            int result_totalTime = twoHopsResults.getInt("total_time");
                
            int result_fid1 = twoHopsResults.getInt("fid1");
            int result_dayOfMonth1 = twoHopsResults.getInt("day_of_month1");
            String result_carrierId1 = twoHopsResults.getString("carrier_id1");
            String result_flightNum1 = twoHopsResults.getString("flight_num1");
            String result_originCity1 = twoHopsResults.getString("origin_city1");
            String result_destCity1 = twoHopsResults.getString("dest_city1");
            int result_time1 = twoHopsResults.getInt("actual_time1");
            int result_capacity1 = twoHopsResults.getInt("capacity1");
            int result_price1 = twoHopsResults.getInt("price1");
            
            int result_fid2 = twoHopsResults.getInt("fid2");
            int result_dayOfMonth2 = twoHopsResults.getInt("day_of_month2");
            String result_carrierId2 = twoHopsResults.getString("carrier_id2");
            String result_flightNum2 = twoHopsResults.getString("flight_num2");
            String result_originCity2 = twoHopsResults.getString("origin_city2");
            String result_destCity2 = twoHopsResults.getString("dest_city2");
            int result_time2 = twoHopsResults.getInt("actual_time2");
            int result_capacity2 = twoHopsResults.getInt("capacity2");
            int result_price2 = twoHopsResults.getInt("price2");

            String itinerary = "ID: " + result_fid1 + " Day: " + result_dayOfMonth1 + " Carrier: " + result_carrierId1 + " Number: " + result_flightNum1
                + " Origin: " + result_originCity1 + " Dest: " + result_destCity1 + " Duration: " + result_time1
                + " Capacity: " + result_capacity1 + " Price: " + result_price1 + "\n"
                + "ID: " + result_fid2 + " Day: " + result_dayOfMonth2 + " Carrier: " + result_carrierId2 + " Number: " + result_flightNum2
                + " Origin: " + result_originCity2 + " Dest: " + result_destCity2 + " Duration: " + result_time2
                + " Capacity: " + result_capacity2 + " Price: " + result_price2 + "\n";
            int totalTime = result_time1 + result_time2;
            String flightDetails = ": 2 flight(s), " + totalTime + " minutes\n";
            // 0 for date, 1 for itinerary, 2 for price, 3 for flight title, 4 for fid1, 5 for fid2
            String[] itineraryDetails = {"" + result_dayOfMonth1, itinerary, "" + (result_price1 + result_price2), flightDetails, result_fid1 + "", result_fid2 + ""};          
            if (!sortedByTime.containsKey(totalTime)) {
              sortedByTime.put(totalTime, new ArrayList());
            }
            sortedByTime.get(totalTime).add(itineraryDetails);
            count++;
          }
          twoHopsResults.close();
        }
        // read-only, commit
        commitStatement.executeUpdate();
        conn.setAutoCommit(true);
      } catch (SQLException ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
          return transaction_search(originCity, destinationCity, 
              directFlight, dayOfMonth, numberOfItineraries);
        } catch (Exception e) {
          e.printStackTrace();
          return "Failed to search\n";
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return "Failed to search\n";
    }
    
    if (count == 0) {
      return "No flights match your selection\n";
    } else {
      int itineraryId = 0;
      for (ArrayList<String[]> list : sortedByTime.values()) {
        if (list.size() == 1) {
          // append to the string returned by this method, add to itineraries
          String[] itineraryDetails = list.get(0);
          sb.append("Itinerary " + itineraryId + itineraryDetails[3] + itineraryDetails[1]);
          itineraries.put(itineraryId, itineraryDetails);
          itineraryId++;
        } else {
          // sort all itineraryDetails by fid1, enough to seperate direct and indirect flights, 
          // fid of direct != fid1 of indirect, because fid is unique
          TreeMap<Integer, ArrayList<String[]>> sortedFid1 = new TreeMap();
          for (int i = 0; i < list.size(); i++) {
            String[] itineraryDetails = list.get(i); 
            int fid1 = Integer.parseInt(itineraryDetails[4]);
            if (!sortedFid1.containsKey(fid1)) {
              sortedFid1.put(fid1, new ArrayList());
            }
            sortedFid1.get(fid1).add(itineraryDetails);
          }
          
          for (ArrayList<String[]> list1 : sortedFid1.values()) {
            if (list1.size() == 1) {
              // append to the string returned by this method, add to itineraries
              String[] itineraryDetails = list1.get(0);
              sb.append("Itinerary " + itineraryId + itineraryDetails[3] + itineraryDetails[1]);
              itineraries.put(itineraryId, itineraryDetails);
              itineraryId++;
            } else { // sort indirect flights with same total time and fid1
              TreeMap<Integer, String[]> sortedFid2 = new TreeMap();
              for (int i = 0; i < list1.size(); i++) {
                String[] itineraryDetails = list1.get(i); 
                int fid2 = Integer.parseInt(itineraryDetails[5]);
                sortedFid2.put(fid2, itineraryDetails);
              }
              
              for (String[] itineraryDetails : sortedFid2.values()) {
                // append to the string returned by this method, add to itineraries
                sb.append("Itinerary " + itineraryId + itineraryDetails[3] + itineraryDetails[1]);
                itineraries.put(itineraryId, itineraryDetails);
                itineraryId++;
              }
            }
          }
        }
      } 
      return sb.toString();
    }   
  }

  /**
   * Implements the book itinerary function.
   *
   * @param itineraryId ID of the itinerary to book. This must be one that is
   *                    returned by search in the current session.
   *
   * @return If the user is not logged in, then return "Cannot book reservations,
   *         not logged in\n". If try to book an itinerary with invalid ID, then
   *         return "No such itinerary {@code itineraryId}\n". If the user already
   *         has a reservation on the same day as the one that they are trying to
   *         book now, then return "You cannot book two flights in the same
   *         day\n". For all other errors, return "Booking failed\n".
   *
   *         And if booking succeeded, return "Booked flight(s), reservation ID:
   *         [reservationId]\n" where reservationId is a unique number in the
   *         reservation system that starts from 1 and increments by 1 each time a
   *         successful reservation is made by any user in the system.
   */
  public String transaction_book(int itineraryId) {
    // TODO: YOUR CODE HERE
    if (login == false) {
      return "Cannot book reservations, not logged in\n";
    }
    if (itineraries == null || !itineraries.containsKey(itineraryId)) {
      return "No such itinerary " + itineraryId + "\n";
    }
    
    // itinerary details
    String[] itineraryDetails = itineraries.get(itineraryId);
    int newDate = Integer.parseInt(itineraryDetails[0]);
    String itinerary = itineraryDetails[1];
    int totalPrice = Integer.parseInt(itineraryDetails[2]);
    int fid1 = Integer.parseInt(itineraryDetails[4]);
    String fid2 = itineraryDetails[5];
  
    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try { 
        // check not the same day
        String reservationsDatesSQL = "SELECT date FROM Reservations WHERE username = \'" + user + "\'";
        Statement reservationsDatesStatement = conn.createStatement();
        ResultSet reservationsDatesResults = reservationsDatesStatement.executeQuery(reservationsDatesSQL);
             
        while (reservationsDatesResults.next()) {
          int date = reservationsDatesResults.getInt("date");
          if (date == newDate) {
            // failure, rollback
            rollbackStatement.executeUpdate();
            conn.setAutoCommit(true);
            return "You cannot book two flights in the same day\n";
          }
        }
        reservationsDatesResults.close();
             
        // check capacity
        int booked1 = getBooked(fid1);
        int capacity1 = checkFlightCapacity(fid1);
        if (booked1 == capacity1) {
          // failure, rollback
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          return "Booking failed\n";
        }
        int booked2 = -1;
        int newFid2 = -1;
        if (!fid2.equals("null")) {
          newFid2 = Integer.parseInt(fid2);
          booked2 = getBooked(newFid2);
          int capacity2 = checkFlightCapacity(newFid2);
          if (booked2 == capacity2) {
            // failure, rollback
            rollbackStatement.executeUpdate();
            conn.setAutoCommit(true);
            return "Booking failed\n";
          }
        }
        
        // get reservation id
        int reservationId = 0;
        String reservationIdSQL = "SELECT next_id FROM ID";
        Statement reservationIdStatement = conn.createStatement();
        ResultSet reservationIdResults = reservationIdStatement.executeQuery(reservationIdSQL); 
        if (reservationIdResults.next()) {
          reservationId = reservationIdResults.getInt("next_id");
          reservationIdResults.close();      
        } else { // no tuple in ID table
          newIdStatement.executeUpdate();
          reservationId = 1;
        }
        
        // create reservation
        insertReservationStatement.clearParameters();
        insertReservationStatement.setInt(1, reservationId);
        insertReservationStatement.setString(2, itinerary);
        insertReservationStatement.setInt(3, newDate);
        insertReservationStatement.setInt(4, fid1);
        if (!fid2.equals("null")) { // has second hop
          insertReservationStatement.setInt(5, newFid2);
        } else {
          insertReservationStatement.setInt(5, -1);
        }
        insertReservationStatement.setInt(6, totalPrice);
        insertReservationStatement.setInt(7, 0); // unpaid
        insertReservationStatement.setString(8, user);
        insertReservationStatement.executeUpdate();
        //update reservation id
        updateIdStatement.clearParameters();
        updateIdStatement.setInt(1, (reservationId + 1));
        updateIdStatement.executeUpdate();
        
        // change booked
        if (booked1 == -1) { // insert booked if does not exist before
          insertBooked(fid1);
        } else { // update
          updateBooked(fid1, booked1 + 1);
        }
        if (!fid2.equals("null")) { // there is the second hop
          if (booked2 == -1) { 
            insertBooked(newFid2);
          } else { 
            updateBooked(newFid2, booked2 + 1);
          }
        }
        
        // success, commit
        commitStatement.executeUpdate();
        conn.setAutoCommit(true);
        
        return "Booked flight(s), reservation ID: " + reservationId + "\n";
      } catch (SQLException ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
          //return transaction_book(itineraryId);
          return "Booking failed\n";
        } catch (Exception e) { // "book 2 flights with 2 users" will always enter here
          e.printStackTrace();
          //return "Booking failed\n";
          return transaction_book(itineraryId);
        }
      } 
    } catch (Exception e) {
      e.printStackTrace();
      return "Booking failed\n";
    }
  }

  /**
   * Implements the pay function.
   *
   * @param reservationId the reservation to pay for.
   *
   * @return If no user has logged in, then return "Cannot pay, not logged in\n"
   *         If the reservation is not found / not under the logged in user's
   *         name, then return "Cannot find unpaid reservation [reservationId]
   *         under user: [username]\n" If the user does not have enough money in
   *         their account, then return "User has only [balance] in account but
   *         itinerary costs [cost]\n" For all other errors, return "Failed to pay
   *         for reservation [reservationId]\n"
   *
   *         If successful, return "Paid reservation: [reservationId] remaining
   *         balance: [balance]\n" where [balance] is the remaining balance in the
   *         user's account.
   */
  public String transaction_pay(int reservationId) {
    // TODO: YOUR CODE HERE
    if (login == false) { // did not login
      return "Cannot pay, not logged in\n";
    }
    
    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try {
        // get reservation's price with the given id and the given user
        reservationPriceStatement.clearParameters();
        reservationPriceStatement.setInt(1, reservationId);
        reservationPriceStatement.setString(2, user);
        ResultSet priceResult = reservationPriceStatement.executeQuery();
        if (priceResult.next()) {
          int totalPrice = priceResult.getInt("total_price");
          priceResult.close();
          
          // get user's balance
          int balance = getBalance();
          
          if (balance >= totalPrice) { // enough money
            // update the reservation to paid
            updatePaidStatement.clearParameters();
            updatePaidStatement.setInt(1, reservationId);
            updatePaidStatement.executeUpdate();
            
            // update user's balance
            int leftBalance = balance - totalPrice;
            updateBalance(leftBalance);
            
            // success, commit
            commitStatement.executeUpdate();
            conn.setAutoCommit(true);
            
            return "Paid reservation: " + reservationId + " remaining balance: " + leftBalance + "\n";
          } else { // no enough money
            // failure, rollback
            rollbackStatement.executeUpdate();
            conn.setAutoCommit(true);
            
            return "User has only " + balance + " in account but itinerary costs " + totalPrice + "\n";
          }
        } else {
          // failure, rollback
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          
          return "Cannot find unpaid reservation " + reservationId + " under user: " + user + "\n";
        }      
      } catch (SQLException ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
          return transaction_pay(reservationId);
          //return "Failed to pay for reservation " + reservationId + "\n";
        } catch (Exception e) {
          e.printStackTrace();
          return transaction_pay(reservationId);
          //return "Failed to pay for reservation " + reservationId + "\n";
        }
      }   
    } catch (Exception e) {
      e.printStackTrace();
      return "Failed to pay for reservation " + reservationId + "\n";
    }
  }

  /**
   * Implements the reservations function.
   *
   * @return If no user has logged in, then return "Cannot view reservations, not
   *         logged in\n" If the user has no reservations, then return "No
   *         reservations found\n" For all other errors, return "Failed to
   *         retrieve reservations\n"
   *
   *         Otherwise return the reservations in the following format:
   *
   *         Reservation [reservation ID] paid: [true or false]:\n" [flight 1
   *         under the reservation] [flight 2 under the reservation] Reservation
   *         [reservation ID] paid: [true or false]:\n" [flight 1 under the
   *         reservation] [flight 2 under the reservation] ...
   *
   *         Each flight should be printed using the same format as in the
   *         {@code Flight} class.
   *
   * @see Flight#toString()
   */
  public String transaction_reservations() {
    // TODO: YOUR CODE HERE
    if (login == false) { // not login
      return "Cannot view reservations, not logged in\n";
    }
    
    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try {
        // find reservations of this user
        getReservationsStatement.clearParameters();
        getReservationsStatement.setString(1, user);
        ResultSet reservationsResults = getReservationsStatement.executeQuery(); 
        String reservationText = "";
        while (reservationsResults.next()) {
          int id = reservationsResults.getInt("id");
          String itinerary = reservationsResults.getString("itinerary");
          String paid = null;
          if (reservationsResults.getInt("paid") == 1) { // paid = 1, true
            paid = "true";
          } else {
            paid = "false";
          }
          reservationText += "Reservation " + id + " paid: " + paid + ":\n" + itinerary;
        }
        reservationsResults.close();
        
        // read-only, commit
        commitStatement.executeUpdate();
        conn.setAutoCommit(true);
        
        if (reservationText.equals("")) { // no reservation found
          return "No reservations found\n";
        } else {
          return reservationText;
        }    
      } catch (SQLException ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
          return transaction_reservations();
        } catch (Exception e) {
          e.printStackTrace();
          return "Failed to retrieve reservations\n";
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return "Failed to retrieve reservations\n";
    }
  }

  /**
   * Implements the cancel operation.
   *
   * @param reservationId the reservation ID to cancel
   *
   * @return If no user has logged in, then return "Cannot cancel reservations,
   *         not logged in\n" For all other errors, return "Failed to cancel
   *         reservation [reservationId]\n"
   *
   *         If successful, return "Canceled reservation [reservationId]\n"
   *
   *         Even though a reservation has been canceled, its ID should not be
   *         reused by the system.
   */
  public String transaction_cancel(int reservationId) {
    // TODO: YOUR CODE HERE
    if (login == false) { // no user login
      return "Cannot cancel reservations, not logged in\n";
    }
    
    try {
      // open transaction
      conn.setAutoCommit(false);
      beginTransacStatement.executeUpdate();
      
      try {
        pricePaidStatement.clearParameters();
        pricePaidStatement.setInt(1, reservationId);
        pricePaidStatement.setString(2, user); 
        ResultSet results = pricePaidStatement.executeQuery();
        
        if (results.next()) {
          int totalPrice = results.getInt("total_price");
          int paid = results.getInt("paid");
          int fid1 = results.getInt("fid1");
          int fid2 = results.getInt("fid2");
          results.close();
          
          if (paid == 1) { // paid = 1, true
            int balance = getBalance();
            updateBalance(balance + totalPrice);
          }
          
          // update booked
          int booked1 = getBooked(fid1);
          updateBooked(fid1, booked1 - 1);
          if (fid2 != -1) { // there is the second hop
            int booked2 = getBooked(fid2);
            updateBooked(fid2, booked2 - 1);
          }
          
          // delete reservation
          deleteReservationStatement.clearParameters();
          deleteReservationStatement.setInt(1, reservationId);
          deleteReservationStatement.executeUpdate();
            
          // success, commit
          commitStatement.executeUpdate();
          conn.setAutoCommit(true);
          
          return "Canceled reservation " + reservationId + "\n";
        } else { // no such reservation with the given id and the given user
          // failure, rollback
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          
          return "Failed to cancel reservation " + reservationId + "\n";
        } 
      } catch (SQLException ex) {
        try {
          rollbackStatement.executeUpdate();
          conn.setAutoCommit(true);
          ex.printStackTrace();
          return "Failed to cancel reservation " + reservationId + "\n";
        } catch (Exception e) {
          e.printStackTrace();
          return "Failed to cancel reservation " + reservationId + "\n";
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return "Failed to cancel reservation " + reservationId + "\n";
    }
  }
  
  //get user's balance
  private int getBalance() throws SQLException {
    getBalanceStatement.clearParameters();
    getBalanceStatement.setString(1, user);
    ResultSet balanceResult = getBalanceStatement.executeQuery();
    balanceResult.next();
    int balance = balanceResult.getInt("balance");
    balanceResult.close();
    return balance;
    /*
    try {
      getBalanceStatement.clearParameters();
      getBalanceStatement.setString(1, user);
      ResultSet balanceResult = getBalanceStatement.executeQuery();
      balanceResult.next();
      int balance = balanceResult.getInt("balance");
      balanceResult.close();
      return balance;
    } catch (SQLException e) {
      e.printStackTrace();
      return -1;
    } 
    */
  }
  
  //update user's balance
  private void updateBalance(int newBalance) {
    try {
      updateBalanceStatement.clearParameters();  
      updateBalanceStatement.setInt(1, newBalance);
      updateBalanceStatement.setString(2, user);
      updateBalanceStatement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } 
  }
  
  /**
   * Example utility function that uses prepared statements
   */
  private int checkFlightCapacity(int fid) throws SQLException {
    checkFlightCapacityStatement.clearParameters();
    checkFlightCapacityStatement.setInt(1, fid);
    ResultSet results = checkFlightCapacityStatement.executeQuery();
    results.next();
    int capacity = results.getInt("capacity");
    results.close();

    return capacity;
  }
  
  // check capacity
  private int getBooked(int fid) throws SQLException {
    getBookedStatement.clearParameters();
    getBookedStatement.setInt(1, fid);
    ResultSet results = getBookedStatement.executeQuery();
    if (results.next()) {
      int booked = results.getInt("booked");
      results.close();
      return booked;
    } else {
      results.close();
      return -1;
    }
  }
  
  private void insertBooked(int fid) throws SQLException {
    insertBookedStatement.clearParameters();
    insertBookedStatement.setInt(1, fid);
    insertBookedStatement.executeUpdate();
    /*
    try {
      insertBookedStatement.clearParameters();
      insertBookedStatement.setInt(1, fid);
      insertBookedStatement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } 
    */
  }
 
  private void updateBooked(int fid, int booked) throws SQLException {
    updateBookedStatement.clearParameters();
    updateBookedStatement.setInt(1, booked);
    updateBookedStatement.setInt(2, fid);
    updateBookedStatement.executeUpdate();
    /*
    try {
      updateBookedStatement.clearParameters();
      updateBookedStatement.setInt(1, booked);
      updateBookedStatement.setInt(2, fid);
      updateBookedStatement.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } */
  }

  /**
   * A class to store flight information.
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: " + flightNum + " Origin: "
          + originCity + " Dest: " + destCity + " Duration: " + time + " Capacity: " + capacity + " Price: " + price;
    }
  }
}
