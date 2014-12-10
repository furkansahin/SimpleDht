package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
	import android.database.sqlite.SQLiteDatabase;
	import android.database.sqlite.SQLiteOpenHelper;

	/*Class for SQLite Database Operations*/
	public class DatabaseHelper extends SQLiteOpenHelper  
	{
		public static final String Table="ChatMessenger_table";
		public static final String DATABASE_NAME = "Chat";		//declare database Name
		static final int DATABASE_VERSION = 1;		//declare database version
		public static final String SQL_CREATE_MAIN = "CREATE TABLE " +
			    "ChatMessenger_table " +                       // Table's name
			    "(" +                           // The columns in the table
			    " key TEXT," +
			    " value TEXT"+
			     ")";

		/*Fetch Database name, Getter*/
		public String getDatabaseName() 
		{
			return DATABASE_NAME;
		}

		
		public static String getTable() {
			return Table;
		}


		public DatabaseHelper(Context context) 
		{
			super(context, DATABASE_NAME, null, DATABASE_VERSION);
			// TODO Auto-generated constructor stub
		}

		
		public void onCreate(SQLiteDatabase db) 
		{
			// TODO Auto-generated method stub
			db.execSQL(SimpleDhtProvider.CREATE_TABLE);		//create table
			
		}
		

		
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			// TODO Auto-generated method stub
			db.execSQL("drop if exists Table "+" "+SimpleDhtProvider.TABLE_NAME);		//if table already exists
			onCreate(db);
			
		}
		

	}


