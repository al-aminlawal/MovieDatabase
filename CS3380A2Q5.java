import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.util.Scanner;


public class CS3380A2Q5 {
    static Connection connection;

    public static void main(String[] args) throws Exception {
	
		// startup sequence
		MyDatabase db = new MyDatabase();
		//db.printAccounts();
		doStuff(db);

		System.out.println("Exiting...");
	}
	
	public static void doStuff(MyDatabase db){

		String name = "Snowball Grottobow";
		String link = "bz4bnJ77um";
		try{
			Scanner sc = new Scanner(System.in);
			System.out.println("Gimme an Elf name: ");
			String maybeName = sc.nextLine();
			System.out.println("Gimme a CheerTube link");
			String maybeLink = sc.nextLine();

			if (maybeName.length() > 0)
				name = maybeName;
			if (maybeLink.length() > 0)
				link = maybeLink;
			sc.close();
		}
		catch(Exception e){
			System.out.println("Using defaults, loser.");
		}
		db.getAccountForElfName(name);
		db.getBillsForAccount(name);
		db.getViewsForLink(link);
		db.getVideosAndViews(name);
		db.creatorIsOnlyViewer();
	}
}

class MyDatabase{
	private Connection connection;
	private final String accountsTXT = "accounts.txt";
	private final String videosTXT = "videos.txt";
	private final String viewsTXT = "views.txt";

	public MyDatabase(){
		try {
			Class.forName("org.hsqldb.jdbcDriver");
			// creates an in-memory database
			connection = DriverManager.getConnection("jdbc:hsqldb:mem:mymemdb", "SA", "");

			createTables();
			readInDataAccountsAndBills();
			readInDataVideos();
			readInDataViewersAndAccountInfo();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace(System.out);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	private void createTables(){
		// To be completed
		String accounts = "create table accounts ( "+
			" accountID integer,"+
            " billingAddress VARCHAR(100),"+
            " primary key(accountID)" +
			")";

		try {
            connection.createStatement().executeUpdate(accounts);

            String bills = "create table bills ("
                + " billID integer," 
                + " amount integer,"
                + " accountID integer,"
                + " primary key(billID),"
                + " foreign key (accountID) references accounts);";

            connection.createStatement().executeUpdate(bills);


			String viewerAccount = "create table viewerAccount ("
					+ " viewerName CHAR(100),"
					+ " accountID integer,"
					+ " primary key(viewerName),"
					+ " foreign key (accountID) references accounts)";

			connection.createStatement().executeUpdate(viewerAccount);


			String videos = "create table videos "
					+ "(link CHAR(50),"
					+ " creatorName VARCHAR(100),"
					+ " videoName VARCHAR(100),"
					+ " duration integer,"
					+ " primary key(link))";

			connection.createStatement().executeUpdate(videos);


			String accountInfo = "create table accountInformation ("
					+ " accountID integer,"
					+ " time integer,"
					+ " link CHAR(50),"
					+ " primary key(accountID,time,link),"
					+ " foreign key (accountID) references accounts,"
					+ " foreign key (link) references videos)";

			connection.createStatement().executeUpdate(accountInfo);
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}

	}

	public int getAccountForElfName(String elfName){
		int accountID = -1;
		/*
		 * To be CORRECTED and completed. Just an example of how this can work. You will have to add more tables to the FROM statement
		 */
		System.out.println("Q1 - account for " + elfName);
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"Select accountID from viewerAccount where viewerName =?;"
			);
			pstmt.setString(1, elfName);

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				int aID = resultSet.getInt("accountID");
				accountID = aID;
				System.out.println(elfName + " is associated with account " + aID);
			}
			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
		return accountID;
	}

	public void getBillsForAccount(String elfName){
		int accountID = getAccountForElfName(elfName);
		/*
		 * To be CORRECTED and completed. Just an example of how this can work. You will have to add more tables to the FROM statement
		 */
		System.out.println("Q2 - Bills for " + elfName);
		try{
			PreparedStatement pstmt = connection.prepareStatement(
					"Select accountID, billID, amount from bills where accountID=?;"
			);
			pstmt.setInt(1, accountID);

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				int bID = resultSet.getInt("billID");
				int amID = resultSet.getInt("amount");
				System.out.println(elfName + " has bill " + bID + " which is for " + amID + "c");
			}
			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getViewsForLink(String link){
		/*
		 * To be CORRECTED and completed. Just an example of how this can work. You will have to add more tables to the FROM statement
		 */
		System.out.println("Q3 - Views for videos with link " + link);
		try{
			PreparedStatement pstmt = connection.prepareStatement(
					"Select videoName from videos join accountInformation on videos.link = accountInformation.link where link=?;"
			);
			pstmt.setString(1, link);

			ResultSet resultSet = pstmt.executeQuery();
			int count = 0;
			boolean obtained = false;
			String vidName = "";
			while (resultSet.next()) {
				count = count +1;
				if (!obtained) {
					vidName = resultSet.getString("videoName");
					obtained = true;
				}
			}
			System.out.println(vidName + " has " + count + " views");
			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void getVideosAndViews(String elfName){
		int accountID = getAccountForElfName(elfName);

		System.out.println("Q4 - videos for " + elfName + " number of views ");
		try{
			PreparedStatement pstmt = connection.prepareStatement(
					"Select videoName from videos where creatorName=?;"
			);
			pstmt.setString(1, elfName);

			ResultSet resultSet = pstmt.executeQuery();
			boolean obtained = false;
			String vidName = "";
			while (resultSet.next()) {
				vidName = resultSet.getString("videoName");
				PreparedStatement pstmt2 = connection.prepareStatement(
						"Select videoName from videos join accountInformation on videos.link = accountInformation.link where videoName=?;"
				);
				pstmt2.setString(1, vidName);
				ResultSet resultSet2 = pstmt2.executeQuery();
				int count = 0;
				while (resultSet2.next()) {
					count++;
				}
				System.out.println(elfName + ";'s video" + vidName + " has " + count + " views");
				count = 0;
			}
			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public void creatorIsOnlyViewer(){
		System.out.println("Q5 - views of videos with no other viewers than the creator ");

		try{

			// and viewerName not equal to any other thing
			/*
			PreparedStatement pstmt = connection.prepareStatement(
					"Select videoName from videos video where video.creatorName in (select viewerName from accountInformation natural join viewerAccount where viewerName = creatorName and viewerName not in (select viewerName from viewerAccount where viewerName != video.creatorName));");

			 */

			/*
			PreparedStatement pstmt = connection.prepareStatement(
					"Select videoName, creatorName from videos video where video.creatorName not in (select viewerName from viewerAccount natural join accountInformation where viewerName != video.creatorName));");
			 */

			/*
			PreparedStatement pstmt = connection.prepareStatement(
					"Select videoName from videos video natural join accountInformation natural join viewerAccount where video.creatorName = viewerName and viewerName not in (select viewerName from viewerAccount where viewerName != video.creatorName);");

			ResultSet resultSet = pstmt.executeQuery();
			 */
			/*
			PreparedStatement pstmt = connection.prepareStatement("Select videoName from videos video natural join accountInformation natural join viewerAccount where " +
					"viewerName = creatorName and video.videoName not in (Select videoName from videos natural join accountInformation natural join viewerAccount where " +
					"video.creatorName != viewerAccount.viewerName);");

			ResultSet resultSet = pstmt.executeQuery();
			 */


			PreparedStatement pstmt = connection.prepareStatement("Select videoName from videos video natural join accountInformation natural join viewerAccount where viewerName = creatorName and " +
					"videoName not in (Select videoName from videos natural join accountInformation natural join viewerAccount where viewerAccount.viewerName != video.creatorName);");

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				String vidName = resultSet.getString("videoName");
				System.out.println("Video " + vidName + " has no other views");

			}
			pstmt.close();
		}
		catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	private void readInDataAccountsAndBills(){
		// to be corrected and completed

		BufferedReader in = null;

		try {
			in = new BufferedReader((new FileReader(accountsTXT)));

			// throw away the first line - the header
			in.readLine();

			// pre-load loop
			String line = in.readLine();
			while (line != null) {
				// split naively on commas
				// good enough for this dataset!
				String[] parts = line.split(",");
				if(parts.length >= 2) {
					makeAccount(parts[0].trim(), parts[2].trim());
					makeBills(parts[1].trim(), parts[3].trim(), parts[0].trim());
				}
				// get next line
				line = in.readLine();
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void readInDataViewersAndAccountInfo() {
		BufferedReader reader = null;

		try{
			reader = new BufferedReader((new FileReader(viewsTXT)));

			reader.readLine();

			String line = reader.readLine();

			while (line != null) {
				String[] parts = line.split(",");
				if (parts.length > 3) {
					makeViewerAccount(parts[1].trim(), parts[0].trim());
					makeAccountInfo(parts[0].trim(), parts[3].trim(), parts[2].trim());
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void readInDataVideos() {
		BufferedReader reader = null;

		try{
			reader = new BufferedReader((new FileReader(videosTXT)));

			reader.readLine();

			String line = reader.readLine();

			while (line != null) {
				String[] parts = line.split(",");
				if (parts.length == 4) {
					makeVideos(parts[2].trim(), parts[0].trim(), parts[1].trim(), parts[3].trim());
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private int makeAccount(String accountID, String billingAddress){
		/*
		 * Really make or create account. Return the account ID
		 * whether it is new, or give the existing one if it already exists
		 */
		int aID = -1;
		try{
			PreparedStatement pstmt = connection.prepareStatement(
				"Select accountID From  accounts where accountID = ?;"
			);
			pstmt.setInt(1, Integer.parseInt(accountID));

			ResultSet resultSet = pstmt.executeQuery();

			if (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				aID = resultSet.getInt("accountID");			
			}
			else{
				// no record
				// make the new account
                PreparedStatement addAccount = connection.prepareStatement(
					"insert into accounts (accountID, billingAddress) values (?, ?);"
				);


                "delete name from councilor where name = ?;"
						Sam Haverts;

                addAccount.setInt(1, Integer.parseInt(accountID) );
                addAccount.setString(2, billingAddress);
                int numUpdated= addAccount.executeUpdate();
                
				addAccount.close();
            
				resultSet.close();
			}
			pstmt.close();
		}
		catch (SQLException e) {
			System.out.println("Error in " + accountID + " " +billingAddress);
			e.printStackTrace(System.out);
		}

		return aID;
	}

	private int makeBills(String billID, String amount, String accountID){
		int aID = -1;
		try{
			PreparedStatement pstmt = connection.prepareStatement(
					"Select billID From  bills where billID = ?;"
			);
			pstmt.setInt(1, Integer.parseInt(billID));

			ResultSet resultSet = pstmt.executeQuery();

			if (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				aID = resultSet.getInt("billID");
			}
			else{
				// no record
				// make the new account
				PreparedStatement addBill = connection.prepareStatement(
						"insert into bills (billID, amount, accountID) values (?, ?, ?);"

				);

				addBill.setInt(1, Integer.parseInt(billID));
				addBill.setInt(2, Integer.parseInt(amount));
				addBill.setInt(3, Integer.parseInt(accountID));

				int numUpdated= addBill.executeUpdate();

				addBill.close();

				resultSet.close();
			}
			pstmt.close();
		}
		catch (SQLException e) {
			System.out.println("Error in " + billID + " " + accountID);
			e.printStackTrace(System.out);
		}

		return aID;
	}

	private String makeViewerAccount(String viewerName, String accountID){
		String aID = "";
		try{
			PreparedStatement pstmt = connection.prepareStatement(
					"Select viewerName From viewerAccount where viewerName = ?;"
			);
			pstmt.setString(1, viewerName);


			ResultSet resultSet = pstmt.executeQuery();

			if (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				aID = resultSet.getString("viewerName");
			}
			else{
				// no record
				// make the new account
				PreparedStatement addViewerAccount = connection.prepareStatement(
						"insert into viewerAccount (viewerName, accountID) values (?, ?);"

				);

				addViewerAccount.setString(1, viewerName);
				addViewerAccount.setInt(2, Integer.parseInt(accountID));


				int numUpdated= addViewerAccount.executeUpdate();

				addViewerAccount.close();

				resultSet.close();
			}
			pstmt.close();
		}
		catch (SQLException e) {
			System.out.println("Error in " + viewerName + " " + accountID);
			e.printStackTrace(System.out);
		}

		return aID;
	}

	private int makeAccountInfo(String accountID, String time, String link){
		int aID = -1;
		try{
			PreparedStatement pstmt = connection.prepareStatement(
					"Select time From accountInformation where accountID = ? AND time = ? AND link = ?;"
			);
			pstmt.setInt(1, Integer.parseInt(accountID));
			pstmt.setInt(2, Integer.parseInt(time));
			pstmt.setString(3, link);

			ResultSet resultSet = pstmt.executeQuery();

			if (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				aID = resultSet.getInt("time");
			}
			else{
				// no record
				// make the new account
				PreparedStatement addAccountInfo = connection.prepareStatement(
						"insert into accountInformation (accountID, time, link) values (?, ?, ?);"

				);

				addAccountInfo.setInt(1, Integer.parseInt(accountID));
				addAccountInfo.setInt(2, Integer.parseInt(time));
				addAccountInfo.setString(3, link);


				int numUpdated= addAccountInfo.executeUpdate();

				addAccountInfo.close();

				resultSet.close();
			}
			pstmt.close();
		}
		catch (SQLException e) {
			System.out.println("Error in " + accountID + " " + time + " " + link);
			e.printStackTrace(System.out);
		}

		return aID;
	}

	private String makeVideos(String link, String creatorName, String videoName, String duration){
		String aID = "";
		try{
			PreparedStatement pstmt = connection.prepareStatement(
					"Select link From  videos where link = ?;"
			);
			pstmt.setString(1, link);

			ResultSet resultSet = pstmt.executeQuery();

			if (resultSet.next()) {
				// at least 1 row (hopefully one row!) exists. Get the ID
				aID = resultSet.getString("link");
			}
			else{
				// no record
				// make the new account
				PreparedStatement addVideo = connection.prepareStatement(
						"insert into videos (link, creatorName, videoName, duration) values (?, ?, ?, ?);"

				);
				addVideo.setString(1, link);
				addVideo.setString(2, creatorName);
				addVideo.setString(3, videoName);
				addVideo.setInt(4, Integer.parseInt(duration));


				int numUpdated= addVideo.executeUpdate();

				addVideo.close();

				resultSet.close();
			}
			pstmt.close();
		}
		catch (SQLException e) {
			System.out.println("Error in " + videoName + " " + creatorName + " " + link);
			e.printStackTrace(System.out);
		}

		return aID;
	}

	public void printAccounts() {
		try {

			PreparedStatement pstmt = connection.prepareStatement("Select videoName, viewerName, creatorName from videos natural join accountInformation natural join viewerAccount where " +
					"creatorName != viewerAccount.viewerName;");

			ResultSet resultSet = pstmt.executeQuery();

			while (resultSet.next()) {
				System.out.println(resultSet.getString("videoName") + resultSet.getString("viewerName") + resultSet.getString("creatorName"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
