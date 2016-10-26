package connection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionLauncher {
	
	private static final Logger log = LoggerFactory.getLogger(ConnectionLauncher.class);
	
	public static void main(String[] args) throws SQLException {
		
		Connection connection = ConnectionFactory.getConnectionFactory("localhost", 5432, "postgres", "cuttlefern").getConnection("postgres", "bondstone");
	
		executeQueries(connection);
		
	}
	
	public static void executeQueries(Connection connection) {
		
		try {
			
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("SELECT * FROM app_user");
			
			while(resultSet.next()) {
			
				//look up the value of a column by the 'title' of the column
				String name = resultSet.getString("name");
				
				//or look up the value of a column by the 'index' of the column 
				int id = resultSet.getInt(1);
				
				log.info("Found a user! The users name is [" + name + "] and the users id is [" + id + "]");
			
			}
			
			ResultSet joinResults = connection.createStatement().executeQuery("SELECT * FROM app_user JOIN app_role ON app_user.role_id = app_role.id");
			
			ResultSetMetaData metaData = joinResults.getMetaData();
			int numberOfColumns = metaData.getColumnCount();
			
			while(joinResults.next()) {
				
				log.info("Lets find out what columns the result of our join statement has!");
				
				//watch out! this for loop start as index '1'
				for(int columnIndex = 1; columnIndex <= numberOfColumns; columnIndex++) {
					log.info("Column [" + metaData.getColumnLabel(columnIndex) + "] has a value of [" + joinResults.getString(columnIndex) + "]");
				}
				
				log.info("That means that " + joinResults.getString("name") + " has the role of " + joinResults.getString("role") + "!");
				
				log.info("Let's promote " + joinResults.getString("name") + " to Admin!");				
				promoteToAdmin(joinResults.getInt("id"), connection);
				log.info("Awesome! Now [" + joinResults.getString("name") + "] has been promoted to [" + joinResults.getString("role") + "] !");
				log.debug("<<< wait wtf, what just happened? I thought we promoted that guy? >>>");
			}
			
			log.debug("Lets go check up our changes...");
			
			ResultSet resultOfOne = connection.createStatement().executeQuery("SELECT app_user.id, name, role FROM app_user JOIN app_role ON app_user.role_id=app_role.id WHERE name='Michael Boren'");
			
			int keyToMyHeart = 0;
			while(resultOfOne.next()) {
				keyToMyHeart = resultOfOne.getInt(1);
				log.debug("Looks like [" + resultOfOne.getString("name") + "] is an [" + resultOfOne.getString("role") + "] after all! Hurray!");
			}
			
			log.debug("Just kidding, demote him");
			demoteToGuest(keyToMyHeart, connection);
			
			connection.close();
			log.debug("The End");
			
		} catch(SQLException e) {
			log.error("Uh oh! That was some bad SQL!", e);
		}
		
	}

	private static void demoteToGuest(int key, Connection connection) throws SQLException {
		
		connection.createStatement().execute("UPDATE app_user SET role_id= " + 3 + " WHERE id=" + key);
		
	}

	private static void promoteToAdmin(int key, Connection connection) throws SQLException {
		
		connection.createStatement().execute("UPDATE app_user SET role_id= " + 1 + " WHERE id=" + key);
		
	}
	
}
